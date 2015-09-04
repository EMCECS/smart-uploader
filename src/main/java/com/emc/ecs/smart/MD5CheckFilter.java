package com.emc.ecs.smart;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Computes the MD5 of the upload body and validates the server's checksum on response.
 */
public class MD5CheckFilter extends ClientFilter {
    private static final int MAX_MARK = 256 * 1024 * 1024;
    private static final Logger l4j = Logger.getLogger(MD5CheckFilter.class);

    @Override
    public ClientResponse handle(ClientRequest clientRequest) throws ClientHandlerException {
        if(clientRequest.getEntity() == null) {
            return getNext().handle(clientRequest);
        }

        // First, calculate the entity's MD5
        String md5hex;

        Object entity = clientRequest.getEntity();
        if(entity instanceof byte[]) {
            md5hex = byteArrayMD5((byte[])entity);
        } else if(entity instanceof ByteBuffer) {
            md5hex = byteBufferMD5((ByteBuffer)entity);
        } else if(entity instanceof InputStream) {
            md5hex = inputStreamMD5((InputStream)entity);
        } else {
            throw new ClientHandlerException("Could not handle entity of type " + entity.getClass());
        }

        ClientResponse response = getNext().handle(clientRequest);

        // Extract MD5 from Etag header.
        String responseMd5Hex = getResponseMD5(response);
        if(responseMd5Hex == null) {
            LogMF.warn(l4j, "Could not extract MD5 from response.  Request MD5={0}", md5hex);
            return response;
        }

        // Compare
        if(!responseMd5Hex.toLowerCase().equals(md5hex.toLowerCase())) {
            // Wrap in ClientHandlerException so the RetryFilter can catch it.
            throw new ClientHandlerException(
                    new IOException(String.format("MD5 Mismatch! Request: %s Response: %s", md5hex, responseMd5Hex)));
        }

        LogMF.debug(l4j, "MD5 check ok: {0} = {1}", md5hex, responseMd5Hex);

        return response;
    }

    /**
     * Extracts the MD5 out of the response Etag header.
     * @param response response to parse
     * @return Etag parsed from the response or null if no MD5 could be found.
     */
    private String getResponseMD5(ClientResponse response) {
        String etag = response.getHeaders().getFirst("etag");

        if(etag == null) {
            return null;
        }

        LogMF.debug(l4j, "Response Etag: {0}", etag);

        return null;
    }

    /**
     * Computes the MD5 of a ByteBuffer and rewinds it.
     * @param entity the ByteBuffer to checksum.
     * @return the MD5 hex string.
     */
    public static String byteBufferMD5(ByteBuffer entity) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // Should never happen
            throw new RuntimeException("Could not load MD5", e);
        }

        md5.update(entity);
        entity.rewind();

        return toHexString(md5.digest());
    }

    /**
     * Computes the MD5 of a byte array.
     * @param entity the byte array to checksum.
     * @return the MD5 hex string.
     */
    public static String byteArrayMD5(byte[] entity) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // Should never happen
            throw new RuntimeException("Could not load MD5", e);
        }
        return toHexString(md5.digest(entity));
    }

    /**
     * Converts a byte array to a hex string.
     * @param arr the array to convert.
     * @return the array's contents as a string of hex characters.
     */
    private static String toHexString(byte[] arr) {
        return DatatypeConverter.printHexBinary(arr);
    }

    /**
     * Computes the MD5 of an InputStream.  The Input stream must support mark/reset and there must not be more than
     * MAX_MARK bytes to checksum.
     * @param entity the InputStream whose contents to checksum.
     * @return the MD5 hex string.
     */
    public static String inputStreamMD5(InputStream entity) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // Should never happen
            throw new RuntimeException("Could not load MD5", e);
        }

        entity.mark(MAX_MARK);
        byte[] buffer = new byte[16 * 1024];
        int c = 0;
        try {
            while ((c = entity.read(buffer)) != -1) {
                md5.update(buffer, 0, c);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading InputStream to MD5");
        }

        try {
            entity.reset();
        } catch (IOException e) {
            throw new RuntimeException("Could not reset InputStream!", e);
        }

        return toHexString(md5.digest());
    }


}