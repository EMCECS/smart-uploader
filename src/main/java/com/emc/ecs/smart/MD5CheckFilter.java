package com.emc.ecs.smart;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Computes the MD5 of the upload body and validates the server's checksum on response.
 */
public class MD5CheckFilter extends ClientFilter {
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
            md5hex = MD5Utils.byteArrayMD5((byte[]) entity);
        } else if(entity instanceof ByteBuffer) {
            md5hex = MD5Utils.byteBufferMD5((ByteBuffer) entity);
        } else if(entity instanceof InputStream) {
            md5hex = MD5Utils.inputStreamMD5((InputStream) entity);
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
            LogMF.warn(l4j, "MD5 Mismatch! Request: {0} Response: {1}", md5hex, responseMd5Hex);
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
        // First look for x-emc-content-md5
        String contentMd5 = response.getHeaders().getFirst("x-emc-content-md5");
        if(contentMd5 != null) {
            return contentMd5.toUpperCase();
        }

        // Else, it might be in the Etag.
        String etag = response.getHeaders().getFirst("etag");

        if(etag == null) {
            return null;
        }

        LogMF.debug(l4j, "Response Etag: {0}", etag);

        if(etag.startsWith("\"")) {
            etag = stripQuotes(etag);
        }

        if(etag.endsWith("-")) {
            LogMF.info(l4j, "Etag does not look like an MD5: {0}", etag);
            return null;
        }

        return etag.toUpperCase();
    }

    private String stripQuotes(String etag) {
        return etag.substring(1, etag.length()-1);
    }


}