package com.emc.ecs.smart;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility methods for computing MD5s
 */
public class MD5Utils {
    private static final int MAX_MARK = 256 * 1024 * 1024;

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
    public static String toHexString(byte[] arr) {
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
