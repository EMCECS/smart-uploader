package com.emc.ecs.smart;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by cwikj on 9/4/2015.
 */
public class ByteBufferInputStream extends InputStream {
    private static final Logger l4j = Logger.getLogger(ByteBufferInputStream.class);
    private ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return this.read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int toRead = len - off;
        if(toRead > buffer.remaining()) {
            toRead = buffer.remaining();
        }

        if(toRead == 0) {
            // EOF
            l4j.trace("EOF");
            return -1;
        }

        buffer.get(b, off, toRead);

        return toRead;
    }

    @Override
    public long skip(long n) throws IOException {
        if(n > buffer.remaining()) {
            n = buffer.remaining();
        }
        buffer.position((int)(buffer.position() + n));

        return n;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public synchronized void mark(int readlimit) {
        buffer.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        buffer.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int read() throws IOException {
        throw new RuntimeException("If you're calling read(), you're going to have a bad time.");
    }
}
