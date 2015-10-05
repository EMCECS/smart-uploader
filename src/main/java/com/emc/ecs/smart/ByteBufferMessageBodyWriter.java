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
