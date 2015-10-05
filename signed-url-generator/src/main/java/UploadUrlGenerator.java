/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import org.apache.commons.cli.*;

import java.net.URI;
import java.net.URL;
import java.util.Calendar;

/**
 * Utility application that generates presigned URLs using the AWS S3 SDK.  You can then use these URLs with other
 * applications such as the SmartUploader.
 */
public class UploadUrlGenerator {
    private static final String ENDPOINT_OPTION = "endpoint";
    private static final String ACCESS_KEY_OPTION = "access-key";
    private static final String SECRET_KEY_OPTION = "secret-key";
    private static final String BUCKET_OPTION = "bucket";
    private static final String KEY_OPTION = "key";
    private static final String EXPIRES_OPTION = "expires";
    private static final String VERB_OPTION = "verb";
    private static final String CONTENT_TYPE_OPTION = "content-type";

    public static void main(String[] args) throws Exception {
        Options opts = new Options();

        opts.addOption(Option.builder().longOpt(ENDPOINT_OPTION).required().hasArg().argName("url")
                .desc("Sets the ECS S3 endpoint to use, e.g. https://ecs.company.com:9021").build());
        opts.addOption(Option.builder().longOpt(ACCESS_KEY_OPTION).required().hasArg().argName("access-key")
                .desc("Sets the Access Key (user) to sign the request").build());
        opts.addOption(Option.builder().longOpt(SECRET_KEY_OPTION).required().hasArg().argName("secret")
                .desc("Sets the secret key to sign the request").build());
        opts.addOption(Option.builder().longOpt(BUCKET_OPTION).required().hasArg().argName("bucket-name")
                .desc("The bucket containing the object").build());
        opts.addOption(Option.builder().longOpt(KEY_OPTION).required().hasArg().argName("object-key")
                .desc("The object name (key) to access with the URL").build());
        opts.addOption(Option.builder().longOpt(EXPIRES_OPTION).hasArg().argName("minutes")
                .desc("Minutes from local time to expire the request.  1 day = 1440, 1 week=10080, " +
                        "1 month (30 days)=43200, 1 year=525600.  Defaults to 1 hour (60).").build());
        opts.addOption(Option.builder().longOpt(VERB_OPTION).hasArg().argName("http-verb").type(HttpMethod.class)
                .desc("The HTTP verb that will be used with the URL (PUT, GET, etc).  Defaults to GET.").build());
        opts.addOption(Option.builder().longOpt(CONTENT_TYPE_OPTION).hasArg().argName("mimetype")
                .desc("Sets the Content-Type header (e.g. image/jpeg) that will be used with the request.  " +
                        "Must match exactly.  Defaults to application/octet-stream for PUT/POST and " +
                        "null for all others").build());

        DefaultParser dp = new DefaultParser();

        CommandLine cmd = null;
        try {
            cmd = dp.parse(opts, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("java -jar UploadUrlGenerator-xxx.jar", opts, true);
            System.exit(255);
        }



        URI endpoint = URI.create(cmd.getOptionValue(ENDPOINT_OPTION));
        String accessKey = cmd.getOptionValue(ACCESS_KEY_OPTION);
        String secretKey = cmd.getOptionValue(SECRET_KEY_OPTION);
        String bucket = cmd.getOptionValue(BUCKET_OPTION);
        String key = cmd.getOptionValue(KEY_OPTION);
        HttpMethod method = HttpMethod.GET;
        if(cmd.hasOption(VERB_OPTION)) {
            method = HttpMethod.valueOf(cmd.getOptionValue(VERB_OPTION).toUpperCase());
        }
        int expiresMinutes = 60;
        if(cmd.hasOption(EXPIRES_OPTION)) {
            expiresMinutes = Integer.parseInt(cmd.getOptionValue(EXPIRES_OPTION));
        }
        String contentType = null;
        if(method == HttpMethod.PUT || method == HttpMethod.POST) {
            contentType = "application/octet-stream";
        }

        if(cmd.hasOption(CONTENT_TYPE_OPTION)) {
            contentType = cmd.getOptionValue(CONTENT_TYPE_OPTION);
        }

        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration cc = new ClientConfiguration();
        // Force use of v2 Signer.  ECS does not support v4 signatures yet.
        cc.setSignerOverride("S3SignerType");
        AmazonS3Client s3 = new AmazonS3Client(credentials, cc);
        s3.setEndpoint(endpoint.toString());
        S3ClientOptions s3c = new S3ClientOptions();
        s3c.setPathStyleAccess(true);
        s3.setS3ClientOptions(s3c);

        // Sign the URL
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MINUTE, expiresMinutes);
        GeneratePresignedUrlRequest req = new GeneratePresignedUrlRequest(bucket, key)
                .withExpiration(c.getTime()).withMethod(method);
        if(contentType != null) {
            req = req.withContentType(contentType);
        }
        URL u = s3.generatePresignedUrl(req);
        System.out.printf("URL: %s\n", u.toURI().toASCIIString());
        System.out.printf("HTTP Verb: %s\n", method);
        System.out.printf("Expires: %s\n", c.getTime());
        System.out.println("To Upload with curl:");

        StringBuilder sb = new StringBuilder();
        sb.append("curl ");

        if(method != HttpMethod.GET) {
            sb.append("-X ");
            sb.append(method.toString());
            sb.append(" ");
        }

        if(contentType != null) {
            sb.append("-H \"Content-Type: ");
            sb.append(contentType);
            sb.append("\" ");
        }

        if(method == HttpMethod.POST || method == HttpMethod.PUT) {
            sb.append("-T <filename> ");
        }

        sb.append("\"");
        sb.append(u.toURI().toASCIIString());
        sb.append("\"");

        System.out.println(sb.toString());

        System.exit(0);
    }
}
