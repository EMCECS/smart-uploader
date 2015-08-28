package com.emc.ecs.smart;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Simple retry handler that retries all 5XX errors and IOExceptions
 */
public class RetryFilter extends ClientFilter {
    private static final Logger l4j = Logger.getLogger(RetryFilter.class);

    public static final int INITIAL_DELAY = 500;
    public static final int RETRY_LIMIT = 3;

    @Override
    public ClientResponse handle(ClientRequest clientRequest) throws ClientHandlerException {
        int retryCount = 0;
        InputStream entityStream = null;
        if (clientRequest.getEntity() instanceof InputStream) entityStream = (InputStream) clientRequest.getEntity();
        while (true) {
            try {
                ClientResponse response = getNext().handle(clientRequest);

                if(response.getStatus() > 399) {
                    throw new UniformInterfaceException(response);
                }

                return response;
            } catch (RuntimeException orig) {
                Throwable t = orig;

                // in this case, the exception was wrapped by Jersey
                if (t instanceof ClientHandlerException) t = t.getCause();

                if (!((t instanceof IOException) || (t instanceof UniformInterfaceException))) throw orig;

                if(t instanceof UniformInterfaceException) {
                    ClientResponse response = ((UniformInterfaceException)t).getResponse();
                    if(response.getStatus() < 500) {
                        LogMF.warn(l4j, "Non-retryable HTTP error {0}:{1}", response.getStatus(),
                                response.getStatusInfo().getReasonPhrase());
                        // Not retryable
                        throw orig;
                    }
                }

                // only retry retryLimit times
                if (++retryCount > RETRY_LIMIT) throw orig;

                // attempt to reset InputStream
                if (entityStream != null) {
                    try {
                        if (!entityStream.markSupported()) throw new IOException("stream does not support mark/reset");
                        entityStream.reset();
                    } catch (IOException e) {
                        l4j.warn("could not reset entity stream for retry: " + e);
                        throw orig;
                    }
                }

                // wait for retry delay
                if (INITIAL_DELAY > 0) {
                    int retryDelay = INITIAL_DELAY * (int) Math.pow(2, retryCount - 1);
                    try {
                        LogMF.debug(l4j, "waiting {0}ms before retry", retryDelay);
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException e) {
                        l4j.warn("interrupted while waiting to retry: " + e.getMessage());
                    }
                }

                LogMF.info(l4j, "error received in response [{0}], retrying ({1} of {2})...", t, retryCount, RETRY_LIMIT);
            }
        }
    }

}
