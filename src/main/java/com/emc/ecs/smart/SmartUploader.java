package com.emc.ecs.smart;

import com.emc.rest.smart.LoadBalancer;
import com.emc.rest.smart.SmartClientFactory;
import com.emc.rest.smart.SmartConfig;
import com.emc.rest.smart.ecs.EcsHostListProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.commons.cli.*;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The SmartUploader can use a pre-signed PUT URL to upload a file to ECS.  In particular, it's targeted at large
 * files and utilizes ECS's byte range PUT extensions to upload the object in parallel threads.  The ECS Smart Client
 * is used to implement a software load balancer to the ECS nodes (the DNS entry is queried for all the IP addresses
 * it resolves to) and filters are added to retry failed segments and to verify the checksum of the segments.
 */
public class SmartUploader {
    private static final Logger l4j = Logger.getLogger(SmartUploader.class);
    public static final int DEFAULT_THREADS = 4;
    /* Large segments used to optimize ILEC */
    public static final int LARGE_SEGMENT = 128 * 1024 * 1024;
    /* Small segments to optimize ECS internal buffer sizes */
    public static final int SMALL_SEGMENT = 2 * 1024 * 1024;
    /* Buffer size for simple uploads */
    public static final int CHUNK_SIZE = 65536;

    private static final String THREADS_OPT = "threads";
    private static final String SIMPLE_OPT = "simple";
    private static final String SEGMENT_SIZE_OPT = "segment-size";
    private static final String FILE_OPT = "file";
    private static final String URL_OPT = "url";
    private static final String CONTENT_TYPE_OPT = "content-type";
    private static final String VERIFY_URL_OPT = "verify-url";

    /**
     * Use commons-cli to parse command line arguments and start the upload.
     */
    public static void main(String[] args) {
        Options opts = new Options();

        // Optional params
        opts.addOption(Option.builder().longOpt(THREADS_OPT).hasArg().argName("thread-count")
                .desc("Sets the number of threads to upload concurrently.  Default is " + DEFAULT_THREADS).build());
        opts.addOption(Option.builder().longOpt(SIMPLE_OPT)
                .desc("For comparison purposes, do a simple single-stream upload instead of a concurrent upload.")
                .build());
        opts.addOption(Option.builder().longOpt(SEGMENT_SIZE_OPT).hasArg().argName("MB")
                .desc("Size of each upload segment in megabytes.  Defaults to 2MB for uploads less than 128MB and " +
                        "128MB for objects greater than or equal to 128MB.").build());
        opts.addOption(Option.builder().longOpt(FILE_OPT).hasArg().argName("path-to-file")
                .required().desc("(Required) The path to the file to upload.").build());
        opts.addOption(Option.builder().longOpt(URL_OPT).hasArg().argName("presigned-upload-url").required()
                .desc("URL used to PUT the file to create an object").build());
        opts.addOption(Option.builder().longOpt(CONTENT_TYPE_OPT).hasArg().argName("mimetype")
                .desc("Value to use for the Content-Type header.  Defaults to application/octet-stream.").build());
        opts.addOption(Option.builder().longOpt(VERIFY_URL_OPT).hasArg().argName("url")
                .desc("If specified, read back the object from this URL and compare with the local file.").build());

        DefaultParser dp = new DefaultParser();

        CommandLine cmd = null;
        try {
            cmd = dp.parse(opts, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("java -jar SmartUploader.jar", opts, true);
            System.exit(255);
        }

        // Required options.
        String url = cmd.getOptionValue(URL_OPT);
        String file = cmd.getOptionValue(FILE_OPT);

        SmartUploader uploader = null;
        try {
            uploader = new SmartUploader(new URL(url), new File(file).toPath());
        } catch (MalformedURLException e) {
            System.err.println("Could not parse upload URL: " + e.getMessage());
            System.exit(2);
        }

        if (cmd.hasOption(SEGMENT_SIZE_OPT)) {
            uploader.setSegmentSize(Integer.parseInt(cmd.getOptionValue(SEGMENT_SIZE_OPT)));
        }
        if (cmd.hasOption(THREADS_OPT)) {
            uploader.setThreadCount(Integer.parseInt(cmd.getOptionValue(THREADS_OPT)));
        }
        if (cmd.hasOption(SIMPLE_OPT)) {
            uploader.setSimpleMode(true);
        }
        if(cmd.hasOption(CONTENT_TYPE_OPT)) {
            uploader.setContentType(cmd.getOptionValue(CONTENT_TYPE_OPT));
        }

        if(cmd.hasOption(VERIFY_URL_OPT)) {
            try {
                uploader.setVerifyUrl(new URL(cmd.getOptionValue(VERIFY_URL_OPT)));
            } catch (MalformedURLException e) {
                System.err.println("Could not parse verify URL: " + e.getMessage());
                System.exit(3);
            }
        }

        uploader.doUpload();

        System.exit(0);
    }

    private int threadCount = DEFAULT_THREADS;
    private int segmentSize = -1;
    private boolean simpleMode = false;
    private URL uploadUrl;
    private URL verifyUrl;
    private Path fileToUpload;
    private long fileSize;
    private long bytesUploaded;
    private FileChannel fileChannel;
    private Client client;
    private Queue<ByteBuffer> buffers = new LinkedList<>();
    private boolean failed = false;
    private String contentType = "application/octet-stream";

    public SmartUploader(URL uploadUrl, Path fileToUpload) {
        this.uploadUrl = uploadUrl;
        this.fileToUpload = fileToUpload;
    }

    public void doUpload() {
        if (isSimpleMode()) {
            doSimpleUpload();
        } else {
            doSegmentedUpload();
        }
    }

    /**
     * Performs a segmented upload to ECS using the SmartClient and the ECS byte range PUT extensions.  The upload
     * URL will be parsed and the hostname will be enumerated in DNS to see if it contains multiple 'A' records.  If
     * so, those will be used to populate the software load balancer.
     */
    private void doSegmentedUpload() {
        try {
            long start = System.currentTimeMillis();
            fileSize = Files.size(fileToUpload);

            // Figure out which segment size to use.
            if(segmentSize == -1) {
                if (fileSize >= LARGE_SEGMENT) {
                    segmentSize = LARGE_SEGMENT;
                } else {
                    segmentSize = SMALL_SEGMENT;
                }
            }

            // Expand the host
            String host = uploadUrl.getHost();
            InetAddress addr = InetAddress.getByName(host);
            List<String> ipAddresses = new ArrayList<>();
            try {
                ipAddresses = getIPAddresses(host);
            } catch (NamingException e) {
                LogMF.warn(l4j, "Could not resolve hostname: {0}: {1}.  Using as-is.", host, e);
                ipAddresses.add(host);
//                ipAddresses.add("10.4.0.101");
//                ipAddresses.add("10.4.0.102");
//                ipAddresses.add("10.4.0.103");
//                ipAddresses.add("10.4.0.104");
            }
            LogMF.info(l4j, "Host {0} resolves to {1}", host, ipAddresses);

            // Initialize the SmartClient
            SmartConfig smartConfig = new SmartConfig(ipAddresses.toArray(new String[ipAddresses.size()]));
            // We don't need to update the host list
            smartConfig.setHostUpdateEnabled(false);

            // Configure the load balancer
            Client pingClient = SmartClientFactory.createStandardClient(smartConfig, new URLConnectionClientHandler());
            LoadBalancer loadBalancer = smartConfig.getLoadBalancer();
            EcsHostListProvider hostListProvider = new EcsHostListProvider(pingClient, loadBalancer, null, null);
            hostListProvider.setProtocol(uploadUrl.getProtocol());
            if(uploadUrl.getPort() != -1) {
                hostListProvider.setPort(uploadUrl.getPort());
            }
            smartConfig.setHostListProvider(hostListProvider);

            client = SmartClientFactory.createSmartClient(smartConfig, new URLConnectionClientHandler());

            // Add our retry handler
            client.addFilter(new RetryFilter());

            // Create a FileChannel for the upload
            fileChannel = new RandomAccessFile(fileToUpload.toFile(), "r").getChannel();

            // The first upload is done without a range to create the initial object.
            doUploadSegment(0);

            // See how many more segments we have
            int segmentCount = (int)(fileSize / (long)segmentSize);
            long remainder = fileSize  % segmentSize;
            if(remainder != 0) {
                // Additional bytes at end
                segmentCount++;
            }

            if(segmentCount > 1) {
                // Build a thread pool to upload the segments.
                ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 15, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>());

                for(int i=1; i<segmentCount; i++) {
                    executor.execute(new SegmentUpload(i));
                }

                // Wait for completion
                while(true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(failed) {
                        // Abort!
                        l4j.warn("Error detected, terminating upload");
                        executor.shutdownNow();
                        break;
                    }
                    if(executor.getQueue().isEmpty()) {
                        l4j.info("All tasks complete, awaiting shutdown");
                        try {
                            executor.shutdown();
                            executor.awaitTermination(1, TimeUnit.MINUTES);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }
            }

            // Done!
            long elapsed = System.currentTimeMillis() - start;
            printRate(fileSize, elapsed);

            // Verify
            if(verifyUrl != null) {
                System.out.printf("\nUpload complete... starting verify...\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(4);
        }
    }

    private void doUploadSegment(int segmentNumber) throws IOException {
        // Calculate the segment
        long segmentStart = (long)segmentNumber * (long)segmentSize;
        int segmentLength = segmentSize;
        if(segmentStart + segmentLength > fileSize) {
            segmentLength = (int)(fileSize - segmentStart);
        }
        LogMF.debug(l4j, "Segment {0} is {1} bytes at position {2}", segmentNumber, segmentLength, segmentStart);

        ByteBuffer buf;
        if(segmentLength == segmentSize) {
            buf = getBuffer();
        } else {
            buf = ByteBuffer.allocateDirect(segmentLength);
        }
        int c;
        synchronized (fileChannel) {
            c = fileChannel.read(buf, segmentStart);
        }

        if(c == -1) {
            throw new EOFException("Unexpected EOF at offset " + segmentStart);
        }

        if(c != buf.capacity()) {
            // Didn't read expected number?
            throw new IOException(String.format("Read %d bytes, expected %d at offset %d", c, segmentLength, segmentStart));
        }

        // Make an MD5 so we can use Content-MD5.
        String md5 = calculateMD5(buf);

        // We have our data.  Now upload it.
        WebResource.Builder builder;
        try {
            builder = client.resource(uploadUrl.toURI()).getRequestBuilder();
        } catch (URISyntaxException e) {
            // Shouldn't happen; by now we've already parsed the URI
            throw new RuntimeException("Could not construct request", e);
        }
        builder.header(HttpHeaders.CONTENT_LENGTH, segmentLength)//.header("Content-MD5", md5)
                .header(HttpHeaders.CONTENT_TYPE, contentType);
        if(segmentNumber != 0) {
            builder.header("Range", buildRange(segmentStart, segmentLength));
        }
        builder.put(new ByteBufferInputStream(buf));

        incrementUpload(segmentLength);
        printPercent();

        if(segmentLength == segmentSize) {
            returnBuffer(buf);
        }
    }

    /**
     * Builds the Range header
     */
    private String buildRange(long segmentStart, int segmentLength) {
        return String.format("bytes=%d-%d", segmentStart, segmentStart+segmentLength-1);
    }

    /**
     * Calculates the MD5 of a ByteBuffer
     */
    private String calculateMD5(ByteBuffer buf) {
        return MD5CheckFilter.byteBufferMD5(buf);
    }


    // Try to re-use buffers as much as we can
    private synchronized ByteBuffer getBuffer() {
        if(buffers.isEmpty()) {
            return ByteBuffer.allocateDirect(segmentSize);
        } else {
            return buffers.remove();
        }
    }

    private synchronized void returnBuffer(ByteBuffer b) {
        b.clear();
        buffers.add(b);
    }


    /**
     * Does a standard PUT upload using HttpURLConnection.
     */
    private void doSimpleUpload() {
        try {
            fileSize = Files.size(fileToUpload);
            HttpURLConnection conn = null;
            long start = System.currentTimeMillis();
            try (DigestInputStream is = new DigestInputStream(Files.newInputStream(fileToUpload), MessageDigest.getInstance("MD5"))){
                conn = (HttpURLConnection) uploadUrl.openConnection();
                conn.setFixedLengthStreamingMode(fileSize);
                conn.setDoInput(true);
                conn.setDoOutput(true);
                conn.setInstanceFollowRedirects(false);
                conn.setUseCaches(false);
                conn.setRequestMethod(HttpMethod.PUT);
                conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM);
                OutputStream os = conn.getOutputStream();
                byte[] buf = new byte[CHUNK_SIZE];
                int len;

                while ((len = is.read(buf)) != -1) {
                    os.write(buf, 0, len);
                    bytesUploaded += len;
                    printPercent();
                }
                os.close();

                if (conn.getResponseCode() != ClientResponse.Status.OK.getStatusCode()) {
                    throw new RuntimeException("Unable to upload object content: status=" + conn.getResponseCode());
                } else {
                    List<String> eTags = conn.getHeaderFields().get(HttpHeaders.ETAG);
                    if (eTags == null || eTags.size() < 1) {
                        throw new RuntimeException("Unable to get ETag for uploaded data");
                    } else {
                        byte[] sentMD5 = is.getMessageDigest().digest();
                        String eTag = eTags.get(0);
                        byte[] gotMD5 = DatatypeConverter.parseHexBinary(eTag.substring(1, eTag.length() - 1));
                        if (!Arrays.equals(gotMD5, sentMD5)) {
                            throw new RuntimeException("ETag doesn't match streamed data's MD5.");
                        }
                    }
                }
            } catch (IOException e) {
                throw new Exception("IOException while uploading object content after writing "
                        + bytesUploaded + " of " + fileSize + " bytes: " + e.getMessage());
            } finally {
                if (conn != null)conn.disconnect();
            }

            long elapsed = System.currentTimeMillis() - start;
            printRate(fileSize, elapsed);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class SegmentUpload implements Runnable {
        private int index;

        public SegmentUpload(int index) {
            this.index = index;
        }

        public void run() {
            try {
                doUploadSegment(index);
            } catch(Exception e) {
                System.err.println("Failed to upload segment " + index);
                e.printStackTrace();
                failed = true;
            }
        }

    }

    private static final String MX_ATTRIB = "MX";
    private static final String ADDR_ATTRIB = "A";
    private static String[] ADDR_ATTRIBS = {ADDR_ATTRIB};


    /**
     * Use JNDI to bind to DNS and resolve ALL the 'A' records for a host.
     * @param hostname host to resolve
     * @return the list of IP addresses for the host.
     */
    public List<String> getIPAddresses(String hostname) throws NamingException {
        InitialDirContext idc;

        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        idc = new InitialDirContext(env);

        List<String> ipAddresses = new ArrayList<String>();
        Attributes attrs = idc.getAttributes(hostname, ADDR_ATTRIBS);
        Attribute attr = attrs.get(ADDR_ATTRIB);

        if (attr != null) {
            for (int i = 0; i < attr.size(); i++) {
                ipAddresses.add((String) attr.get(i));
            }
        }

        return ipAddresses;
    }

    private synchronized void incrementUpload(int bytes) {
        bytesUploaded += bytes;
    }

    private synchronized void printPercent() {
        System.out.printf("\r%d / %d (%d %%)", bytesUploaded, fileSize, (bytesUploaded*100 / fileSize));
    }

    private void printRate(long fileSize, long elapsed) {
        System.out.printf("\nUploaded %d bytes in %d ms (%d b/s)\n", fileSize, elapsed, (fileSize / (elapsed/1000)));
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    public boolean isSimpleMode() {
        return simpleMode;
    }

    public void setSimpleMode(boolean simpleMode) {
        this.simpleMode = simpleMode;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public URL getVerifyUrl() {
        return verifyUrl;
    }

    public void setVerifyUrl(URL verifyUrl) {
        this.verifyUrl = verifyUrl;
    }
}
