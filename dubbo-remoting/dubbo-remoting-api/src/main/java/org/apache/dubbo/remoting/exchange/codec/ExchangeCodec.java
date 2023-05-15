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
package org.apache.dubbo.remoting.exchange.codec;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.StreamUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.buffer.ChannelBufferInputStream;
import org.apache.dubbo.remoting.buffer.ChannelBufferOutputStream;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.telnet.codec.TelnetCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.remoting.transport.ExceedPayloadLimitException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link ExchangeCodec}再{@link TelnetCodec}的基础上，提供了处理协议头的能力
 * <p>
 * ExchangeCodec.
 */
public class ExchangeCodec extends TelnetCodec {

    /**
     * 协议头长度，16字节，128位
     * <p>
     * header length.
     */
    protected static final int HEADER_LENGTH = 16;

    /**
     * 协议头的前16位，是固定的魔数值，分为{@link #MAGIC_HIGH}和{@link #MAGIC_LOW}两个字节，每个字节8位，可以根据这两个字节快速判断一个数据包是否为{@code Dubbo}
     * 协议，类似Java字节码文件中的魔数
     * <p>
     * {@link #MAGIC_HIGH}：使用第0-7位比特位，共8位比特
     * {@link #MAGIC_LOW}：使用第8-15位比特位，共8位比特
     * <p>
     * magic header.
     */
    protected static final short MAGIC = (short)0xdabb;
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];

    /**
     * 消息标识位，用于标识当前消息是{@link Request}还是{@link Response}，占一个比特
     * <p>
     * {@link #FLAG_REQUEST}：使用第16位比特位，共1位比特
     * <p>
     * message flag.
     */
    protected static final byte FLAG_REQUEST = (byte)0x80;

    /**
     * twoWay标识符，表示当前消息是单向消息还是双向消息，占一个比特
     * <p>
     * {@link #FLAG_TWOWAY}：使用第17位比特位，共1位比特
     */
    protected static final byte FLAG_TWOWAY = (byte)0x40;

    /**
     * event标识位，表示当前消息是否事件消息，占一个比特
     * <p>
     * {@link #FLAG_EVENT}：使用第18位比特位，共1位比特
     */
    protected static final byte FLAG_EVENT = (byte)0x20;

    /**
     * 用于获取序列化类型的标志位的掩码，占五个比特
     * <p>
     * {@link #FLAG_EVENT}：使用第19-23位比特位，共5位比特
     * <p>
     * 剩余的协议内容：
     * <p>
     * {@code status}：表示响应状态，使用第24-31位比特位，共5位比特，只在{@link Response}时有值
     * {@code requestId}：表示请求id，使用第32-95位比特位，共64位比特，值类型为{@link Long}
     * {@code payloadLength}：表示协议负载数据（{@link Request#mData} 或 {@link Response#mResult}）的长度，使用第96-127位比特位，共32
     * 位比特，值类型为{@link Integer}
     * {@code payload}：协议负载数据内容，由{@link #SERIALIZATION_MASK}代表的序列化器序列后的字节数据进行填充，根据请求类型的不同，会有不同的数据排列：
     * 协议内容为{@link Request}时的排列方案：
     * <ol>
     *     <li>{@code dubbo version}：dubbo版本</li>
     *     <li>{@code interface name}：接口全路径</li>
     *     <li>{@code interface version}：接口版本</li>
     *     <li>{@code method name}：方法名称</li>
     *     <li>{@code method parameter type}：方法参数类型</li>
     *     <li>{@code method arguments}：方法参数值</li>
     *     <li>{@code attachments}：请求携带的附件内容</li>
     * </ol>
     * 协议内容为{@link Response}时的排列方案：
     * <ol>
     *     <li>{@code return type}：返回值类型（不是Class类型，而是返回值的描述）：
     *         <ul>
     *             <li>{@code RESPONSE_WITH_EXCEPTION}：异常返回值</li>
     *             <li>{@code RESPONSE_VALUE}：正常返回值</li>
     *             <li>{@code RESPONSE_NULL_VALUE}：空返回值</li>
     *         </ul>
     *     </li>
     *     <li>{@code return value}：代表返回值的字节，由{@link #SERIALIZATION_MASK}代表的序列化器序列后的字节数据进行填充</li>
     * </ol>
     */
    protected static final int SERIALIZATION_MASK = 0x1f;
    private static final Logger logger = LoggerFactory.getLogger(ExchangeCodec.class);

    public Short getMagicCode() {
        return MAGIC;
    }

    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        if (msg instanceof Request) {
            encodeRequest(channel, buffer, (Request) msg);
        } else if (msg instanceof Response) {
            encodeResponse(channel, buffer, (Response) msg);
        } else {
            super.encode(channel, buffer, msg);
        }
    }

    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        int readable = buffer.readableBytes();
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        buffer.readBytes(header);
        return decode(channel, buffer, readable, header);
    }

    @Override
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] header) throws IOException {
        // check magic number.
        if (readable > 0 && header[0] != MAGIC_HIGH
                || readable > 1 && header[1] != MAGIC_LOW) {
            int length = header.length;
            if (header.length < readable) {
                header = Bytes.copyOf(header, readable);
                buffer.readBytes(header, length, readable - length);
            }
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
            return super.decode(channel, buffer, readable, header);
        }
        // check length.
        if (readable < HEADER_LENGTH) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // get data length.
        int len = Bytes.bytes2int(header, 12);

        // When receiving response, how to exceed the length, then directly construct a response to the client.
        // see more detail from https://github.com/apache/dubbo/issues/7021.
        Object obj = finishRespWhenOverPayload(channel, len, header);
        if (null != obj) {
            return obj;
        }

        checkPayload(channel, len);

        int tt = len + HEADER_LENGTH;
        if (readable < tt) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // limit input stream.
        ChannelBufferInputStream is = new ChannelBufferInputStream(buffer, len);

        try {
            return decodeBody(channel, is, header);
        } finally {
            if (is.available() > 0) {
                try {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Skip input stream " + is.available());
                    }
                    StreamUtils.skipUnusedStream(is);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }

    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {

        // flag是协议头的第三个字节，存储着req/res、twoWay、event和serializationId标识
        // proto是计算出的serializationId
        byte flag = header[2], proto = (byte)(flag & SERIALIZATION_MASK);

        // 计算出请求id，请求id存储在32比特到64比特中
        // get request id.
        long id = Bytes.bytes2long(header, 4);

        // 如果是响应
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            Response res = new Response(id);

            // 计算是否event响应
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(true);
            }

            // 获取响应状态，仅在Response中需要设置和解析
            // get status.
            byte status = header[3];
            res.setStatus(status);
            try {

                // 响应成功，解析数据
                if (status == Response.OK) {
                    Object data;

                    // 解析事件数据：心跳或非心跳
                    if (res.isEvent()) {
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                            // heart beat response data is always null;
                            data = null;
                        } else {
                            data = decodeEventData(channel,
                                CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload),
                                    proto), eventPayload);
                        }
                    }

                    // 解析正常的响应数据
                    else {
                        data = decodeResponseData(channel, CodecSupport.deserialize(channel.getUrl(), is, proto),
                            getRequestData(id));
                    }

                    // 设置数据
                    res.setResult(data);
                }

                // 响应失败，解析错误信息
                else {
                    res.setErrorMessage(CodecSupport.deserialize(channel.getUrl(), is, proto).readUTF());
                }
            } catch (Throwable t) {
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        }

        // 解析请求
        else {
            // decode request.
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion());

            // 设置事件标识
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(true);
            }
            try {
                Object data;

                // 解析事件数据
                if (req.isEvent()) {
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                        // heart beat response data is always null;
                        data = null;
                    } else {
                        data = decodeEventData(channel,
                            CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto),
                            eventPayload);
                    }
                }

                // 解码请求数据
                else {
                    data = decodeRequestData(channel, CodecSupport.deserialize(channel.getUrl(), is, proto));
                }
                req.setData(data);
            } catch (Throwable t) {
                // bad request
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    protected Object getRequestData(long id) {
        DefaultFuture future = DefaultFuture.getFuture(id);
        if (future == null) {
            return null;
        }
        Request req = future.getRequest();
        if (req == null) {
            return null;
        }
        return req.getData();
    }

    /**
     * 写入数据到{@code buffer}中，先写入协议数据，再写入协议头
     * <p>
     * 编码请求：
     * <p>
     * 1、先将数据写入到代表协议头的{@code header}数组中，
     * 2、再将协议负载内容写入到{@code buffer}中，设置协议负载内容长度到 {@code header}中，
     * 3、最后将协议头数据{@code header}写入到{@code buffer}中
     */
    protected void encodeRequest(Channel channel, ChannelBuffer buffer, Request req) throws IOException {

        // 获取序列化器
        Serialization serialization = getSerialization(channel, req);

        // 初始化协议头数组
        // header.
        byte[] header = new byte[HEADER_LENGTH];

        // 设置魔数
        // set magic number.
        Bytes.short2bytes(MAGIC, header);

        // 设置请求/响应标识，和serializationId标识
        // set request and serialization flag.
        header[2] = (byte)(FLAG_REQUEST | serialization.getContentTypeId());

        // twoWay和event标识设置
        if (req.isTwoWay()) {
            header[2] |= FLAG_TWOWAY;
        }
        if (req.isEvent()) {
            header[2] |= FLAG_EVENT;
        }

        // requestId设置
        // set request id.
        Bytes.long2bytes(req.getId(), header, 4);

        // encode request data.
        // 记录当前的写入索引，为协议头留出位置，先将协议数据内容写入到buffer中
        int savedWriteIndex = buffer.writerIndex();
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
        ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);

        // 序列化心跳请求：心跳请求数据为空
        if (req.isHeartbeat()) {
            // heartbeat request data is always null
            bos.write(CodecSupport.getNullBytesOf(serialization));
        }

        // 序列化正常请求数据
        else {

            // 包装bos，获取序列化器对应的ObjectOutput，用于写入数据
            ObjectOutput out = serialization.serialize(channel.getUrl(), bos);

            // 序列化事件请求的数据
            if (req.isEvent()) {
                encodeEventData(channel, out, req.getData());
            }

            // 序列化正常请求的数据
            else {
                encodeRequestData(channel, out, req.getData(), req.getVersion());
            }
            out.flushBuffer();
            if (out instanceof Cleanable) {
                ((Cleanable)out).cleanup();
            }
        }

        // flush数据并关闭输出流
        bos.flush();
        bos.close();

        // 校验负载大小
        int len = bos.writtenBytes();
        checkPayload(channel, len);

        // 写入请求数据的长度到header
        Bytes.int2bytes(len, header, 12);

        // write
        // 重置写索引位置，开始写入协议头
        buffer.writerIndex(savedWriteIndex);
        buffer.writeBytes(header); // write header.

        // 恢复写索引位置到协议数据内容尾部
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }

    /**
     * 编码响应
     * <p>
     * 1、先将数据写入到代表协议头的{@code header}数组中，
     * 2、再将协议负载内容写入到{@code buffer}中，设置协议负载内容长度到 {@code header}中，
     * 3、最后将协议头数据{@code header}写入到{@code buffer}中
     */
    protected void encodeResponse(Channel channel, ChannelBuffer buffer, Response res) throws IOException {
        int savedWriteIndex = buffer.writerIndex();
        try {
            Serialization serialization = getSerialization(channel, res);
            // header.
            byte[] header = new byte[HEADER_LENGTH];

            // 魔数、序列化器id、event、status和requestId的设置
            // set magic number.
            Bytes.short2bytes(MAGIC, header);
            // set request and serialization flag.
            header[2] = serialization.getContentTypeId();
            if (res.isHeartbeat()) {
                header[2] |= FLAG_EVENT;
            }
            // set response status.
            byte status = res.getStatus();
            header[3] = status;
            // set request id.
            Bytes.long2bytes(res.getId(), header, 4);

            // 修改写索引，调整到固定协议头的长度之后
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);

            // 将通道缓存包装成输出流
            ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);

            // encode response data or error message.
            if (status == Response.OK) {
                if (res.isHeartbeat()) {
                    // heartbeat response data is always null
                    bos.write(CodecSupport.getNullBytesOf(serialization));
                } else {

                    // 根据url上配置的序列化器，将通道输出流包装成对应的对象输出流，以便处理请求对象实例
                    ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
                    if (res.isEvent()) {
                        encodeEventData(channel, out, res.getResult());
                    }

                    // 编码响应数据
                    else {
                        encodeResponseData(channel, out, res.getResult(), res.getVersion());
                    }
                    out.flushBuffer();
                    if (out instanceof Cleanable) {
                        ((Cleanable) out).cleanup();
                    }
                }
            } else {

                // 根据url上配置的序列化器，将通道输出流包装成对应的对象输出流，以便处理请求对象实例
                ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
                out.writeUTF(res.getErrorMessage());
                out.flushBuffer();
                if (out instanceof Cleanable) {
                    ((Cleanable)out).cleanup();
                }
            }

            bos.flush();
            bos.close();

            // 计算协议负载的内容大小
            int len = bos.writtenBytes();

            // 判断负载内容是否超出限制
            checkPayload(channel, len);

            // 写入负载内容的大小到header中
            Bytes.int2bytes(len, header, 12);

            // write
            // 重置写索引，写入协议头数据，并根据协议头长度设置写索引
            buffer.writerIndex(savedWriteIndex);
            buffer.writeBytes(header); // write header.
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Throwable t) {
            // clear buffer
            buffer.writerIndex(savedWriteIndex);
            // send error message to Consumer, otherwise, Consumer will wait till timeout.
            if (!res.isEvent() && res.getStatus() != Response.BAD_RESPONSE) {
                Response r = new Response(res.getId(), res.getVersion());
                r.setStatus(Response.BAD_RESPONSE);

                if (t instanceof ExceedPayloadLimitException) {
                    logger.warn(t.getMessage(), t);
                    try {
                        r.setErrorMessage(t.getMessage());
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + t.getMessage() + ", cause: " + e.getMessage(), e);
                    }
                } else {
                    // FIXME log error message in Codec and handle in caught() of IoHanndler?
                    logger.warn("Fail to encode response: " + res + ", send bad_response info instead, cause: " + t.getMessage(), t);
                    try {
                        r.setErrorMessage("Failed to send response: " + res + ", cause: " + StringUtils.toString(t));
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + res + ", cause: " + e.getMessage(), e);
                    }
                }
            }

            // Rethrow exception
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t.getMessage(), t);
            }
        }
    }

    @Override
    protected Object decodeData(ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeRequestData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeResponseData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Override
    protected void encodeData(ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    private void encodeEventData(ObjectOutput out, Object data) throws IOException {
        out.writeEvent(data);
    }

    @Deprecated
    protected void encodeHeartbeatData(ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    protected void encodeRequestData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    protected void encodeResponseData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    @Override
    protected Object decodeData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(channel, in);
    }

    protected Object decodeEventData(Channel channel, ObjectInput in, byte[] eventBytes) throws IOException {
        try {
            if (eventBytes != null) {
                int dataLen = eventBytes.length;
                int threshold = ConfigurationUtils.getSystemConfiguration().getInt("deserialization.event.size", 50);
                if (dataLen > threshold) {
                    throw new IllegalArgumentException("Event data too long, actual size " + dataLen + ", threshold " + threshold + " rejected for security consideration.");
                }
            }
            return in.readEvent();
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Decode dubbo protocol event failed.", e));
        }
    }

    protected Object decodeRequestData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in) throws IOException {
        return decodeResponseData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in, Object requestData) throws IOException {
        return decodeResponseData(channel, in);
    }

    @Override
    protected void encodeData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data);
    }

    private void encodeEventData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    @Deprecated
    protected void encodeHeartbeatData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeHeartbeatData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeResponseData(out, data);
    }

    private Object finishRespWhenOverPayload(Channel channel, long size, byte[] header) {
        int payload = getPayload(channel);
        boolean overPayload = isOverPayload(payload, size);
        if (overPayload) {
            long reqId = Bytes.bytes2long(header, 4);
            byte flag = header[2];
            if ((flag & FLAG_REQUEST) == 0) {
                Response res = new Response(reqId);
                if ((flag & FLAG_EVENT) != 0) {
                    res.setEvent(true);
                }
                // get status.
                byte status = header[3];
                res.setStatus(Response.CLIENT_ERROR);
                String errorMsg = "Data length too large: " + size + ", max payload: " + payload + ", channel: " + channel;
                logger.error(errorMsg);
                res.setErrorMessage(errorMsg);
                return res;
            }
        }
        return null;
    }
}
