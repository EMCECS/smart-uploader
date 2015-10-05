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

import junit.framework.TestCase;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

/**
 * Test the MD5 check methods.
 */
public class MD5CheckFilterTest extends TestCase {
    private static final String TEST_STRING = "Hello World!";
    private static final String TEST_MD5 = "ed076287532e86365e841e92bfc50d8c".toUpperCase();

    public void testHandle() throws Exception {

    }

    public void testByteBufferMD5() throws Exception {
        byte[] data = TEST_STRING.getBytes("us-ascii");
        ByteBuffer bb = ByteBuffer.allocateDirect(data.length);
        bb.put(data);
        bb.rewind();

        MD5CheckFilter cf = new MD5CheckFilter();

        Assert.assertEquals("MD5 mismatch", TEST_MD5, MD5Utils.byteBufferMD5(bb));
    }

    public void testByteArrayMD5() throws Exception {
        byte[] data = TEST_STRING.getBytes("us-ascii");
        MD5CheckFilter cf = new MD5CheckFilter();

        Assert.assertEquals("MD5 mismatch", TEST_MD5, MD5Utils.byteArrayMD5(data));
    }

    public void testInputStreamMD5() throws Exception {
        byte[] data = TEST_STRING.getBytes("us-ascii");
        MD5CheckFilter cf = new MD5CheckFilter();

        Assert.assertEquals("MD5 mismatch", TEST_MD5, MD5Utils.inputStreamMD5(new ByteArrayInputStream(data)));

    }
}