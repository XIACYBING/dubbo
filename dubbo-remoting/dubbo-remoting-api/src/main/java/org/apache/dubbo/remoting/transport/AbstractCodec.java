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
package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;

/**
 * {@link Codec2}的抽象实现，提供以下方法的通用方法实现：
 *
 * @see #getSerialization：基于{@link Channel}上的{@link URL}上的{@link Constants#SERIALIZATION_KEY}参数，通过SPI机制获取当前使用的序列化方式
 * @see #checkPayload：检查编码和解码的数据长度，如果超出限制，则抛出异常
 * @see #isClientSide：当前是Client端
 * @see #isServerSide：当前是Server端
 * <p>
 * AbstractCodec
 */
public abstract class AbstractCodec implements Codec2 {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCodec.class);

    private static final String CLIENT_SIDE = "client";

    private static final String SERVER_SIDE = "server";

    protected static void checkPayload(Channel channel, long size) throws IOException {

        // 获取配置的payload长度限制
        int payload = getPayload(channel);

        // 判断当前数据量大小是否超出限制
        boolean overPayload = isOverPayload(payload, size);

        // 超出限制则抛出异常
        if (overPayload) {
            ExceedPayloadLimitException e = new ExceedPayloadLimitException(
                "Data length too large: " + size + ", max payload: " + payload + ", channel: " + channel);
            logger.error(e);
            throw e;
        }
    }

    protected static int getPayload(Channel channel) {
        int payload = Constants.DEFAULT_PAYLOAD;
        if (channel != null && channel.getUrl() != null) {
            payload = channel.getUrl().getParameter(Constants.PAYLOAD_KEY, Constants.DEFAULT_PAYLOAD);
        }
        return payload;
    }

    protected static boolean isOverPayload(int payload, long size) {
        if (payload > 0 && size > payload) {
            return true;
        }
        return false;
    }

    protected Serialization getSerialization(Channel channel, Request req) {
        return CodecSupport.getSerialization(channel.getUrl());
    }

    protected Serialization getSerialization(Channel channel, Response res) {
        return CodecSupport.getSerialization(channel.getUrl());
    }

    protected Serialization getSerialization(Channel channel) {
        return CodecSupport.getSerialization(channel.getUrl());
    }

    protected boolean isClientSide(Channel channel) {
        String side = (String) channel.getAttribute(SIDE_KEY);
        if (CLIENT_SIDE.equals(side)) {
            return true;
        } else if (SERVER_SIDE.equals(side)) {
            return false;
        }

        // 如果side参数既不是client和server，则重新判断
        else {
            InetSocketAddress address = channel.getRemoteAddress();
            URL url = channel.getUrl();
            boolean isClient = url.getPort() == address.getPort() && NetUtils
                .filterLocalHost(url.getIp())
                .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
            channel.setAttribute(SIDE_KEY, isClient ? CLIENT_SIDE : SERVER_SIDE);
            return isClient;
        }
    }

    protected boolean isServerSide(Channel channel) {
        return !isClientSide(channel);
    }

}
