/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dubbo.remoting.buffer;

import java.nio.ByteBuffer;

/**
 * {@link ChannelBuffer}的工具类
 *
 * @see #buffer：{@link HeapChannelBuffer}的工具方法
 * @see #wrappedBuffer：{@link HeapChannelBuffer}的工具方法
 * @see #dynamicBuffer：{@link DynamicChannelBuffer}的工具方法
 * @see #directBuffer ：{@link ByteBufferBackedChannelBuffer}的工具方法
 * @see #equals(ChannelBuffer, ChannelBuffer) ：比较两个{@link ChannelBuffer}是否相等
 * @see #compare(ChannelBuffer, ChannelBuffer) ：排序两个{@link ChannelBuffer}
 */
public final class ChannelBuffers {

    public static final ChannelBuffer EMPTY_BUFFER = new HeapChannelBuffer(0);

    private ChannelBuffers() {
    }

    public static ChannelBuffer dynamicBuffer() {
        return dynamicBuffer(256);
    }

    public static ChannelBuffer dynamicBuffer(int capacity) {
        return new DynamicChannelBuffer(capacity);
    }

    public static ChannelBuffer dynamicBuffer(int capacity,
                                              ChannelBufferFactory factory) {
        return new DynamicChannelBuffer(capacity, factory);
    }

    public static ChannelBuffer buffer(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity can not be negative");
        }
        if (capacity == 0) {
            return EMPTY_BUFFER;
        }
        return new HeapChannelBuffer(capacity);
    }

    public static ChannelBuffer wrappedBuffer(byte[] array, int offset, int length) {
        if (array == null) {
            throw new NullPointerException("array == null");
        }
        byte[] dest = new byte[length];
        System.arraycopy(array, offset, dest, 0, length);
        return wrappedBuffer(dest);
    }

    public static ChannelBuffer wrappedBuffer(byte[] array) {
        if (array == null) {
            throw new NullPointerException("array == null");
        }
        if (array.length == 0) {
            return EMPTY_BUFFER;
        }
        return new HeapChannelBuffer(array);
    }

    public static ChannelBuffer wrappedBuffer(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return EMPTY_BUFFER;
        }
        if (buffer.hasArray()) {
            return wrappedBuffer(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            return new ByteBufferBackedChannelBuffer(buffer);
        }
    }

    public static ChannelBuffer directBuffer(int capacity) {
        if (capacity == 0) {
            return EMPTY_BUFFER;
        }

        ChannelBuffer buffer = new ByteBufferBackedChannelBuffer(ByteBuffer.allocateDirect(capacity));
        buffer.clear();
        return buffer;
    }

    /**
     * 比较两个{@link ChannelBuffer}是否相同，只需要比较前七个字节即可：在Dubbo的请求和响应中，前七个字节是协议头部分，
     * 包含了请求或响应的消息类型、请求ID等信息。这些信息足以确定一个请求或响应的唯一性。因此，只需要比较前七个字节即可。
     */
    public static boolean equals(ChannelBuffer bufferA, ChannelBuffer bufferB) {
        final int aLen = bufferA.readableBytes();
        if (aLen != bufferB.readableBytes()) {
            return false;
        }

        // 最多比较七个字节 todo 为什么是取余数？如果aLen等于8，岂不是只比较第一个可读字节，这感觉不能作为两个ChannelBuffer相等的依据？
        final int byteCount = aLen & 7;

        int aIndex = bufferA.readerIndex();
        int bIndex = bufferB.readerIndex();

        // 从第一个可读字节开始，比较byteCount次
        for (int i = byteCount; i > 0; i--) {
            if (bufferA.getByte(aIndex) != bufferB.getByte(bIndex)) {
                return false;
            }
            aIndex++;
            bIndex++;
        }

        return true;
    }

    public static int hasCode(ChannelBuffer buffer){
        final int aLen = buffer.readableBytes();
        final int byteCount = aLen & 7;

        int hashCode = 1;
        int arrayIndex = buffer.readerIndex();

        for (int i = byteCount; i > 0; i--) {
            hashCode = 31 * hashCode + buffer.getByte(arrayIndex++);
        }

        if (hashCode == 0) {
            hashCode = 1;
        }

        return hashCode;
    }

    /**
     * 通过比较所有可读字节，得出排序结果
     */
    public static int compare(ChannelBuffer bufferA, ChannelBuffer bufferB) {
        final int aLen = bufferA.readableBytes();
        final int bLen = bufferB.readableBytes();
        final int minLength = Math.min(aLen, bLen);

        int aIndex = bufferA.readerIndex();
        int bIndex = bufferB.readerIndex();

        for (int i = minLength; i > 0; i--) {
            byte va = bufferA.getByte(aIndex);
            byte vb = bufferB.getByte(bIndex);
            if (va > vb) {
                return 1;
            } else if (va < vb) {
                return -1;
            }
            aIndex++;
            bIndex++;
        }

        return aLen - bLen;
    }

}
