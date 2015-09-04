package com.emc.ecs.smart;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * MessageBodyWriter for writing ByteBuffer objects.
 */
@Provider
public class ByteBufferMessageBodyWriter implements MessageBodyWriter<ByteBuffer> {

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return ByteBuffer.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(ByteBuffer byteBuffer, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return byteBuffer.remaining();
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer, Class<?> type, Type genericType, Annotation[] annotations,
                        MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
                        OutputStream entityStream) throws IOException, WebApplicationException {
        byte[] buf = new byte[32*1024];
        while(byteBuffer.remaining() > 0) {
            int toRead = buf.length;
            if(byteBuffer.remaining() < toRead) {
                toRead = byteBuffer.remaining();
            }
            byteBuffer.get(buf, 0, toRead);
            entityStream.write(buf, 0, toRead);
        }
    }
}
