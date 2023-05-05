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
package org.apache.dubbo.remoting;

import org.apache.dubbo.common.URL;

import java.net.InetSocketAddress;

/**
 * 端点：由IP + port确认一个{@link Endpoint}
 * <p>
 * Endpoint. (API/SPI, Prototype, ThreadSafe)
 *
 * @see org.apache.dubbo.remoting.Channel
 * @see org.apache.dubbo.remoting.Client
 * @see RemotingServer
 */
public interface Endpoint {

    /**
     * 获取当前端点关联的URL信息
     * <p>
     * get url.
     *
     * @return url
     */
    URL getUrl();

    /**
     * 获取当前端点对应的{@link Channel}的{@link ChannelHandler}
     * <p>
     * get channel handler.
     *
     * @return channel handler
     */
    ChannelHandler getChannelHandler();

    /**
     * 获取当前端点的本地地址
     * <p>
     * get local address.
     *
     * @return local address.
     */
    InetSocketAddress getLocalAddress();

    /**
     * 发送消息
     * <p>
     * send message.
     */
    void send(Object message) throws RemotingException;

    /**
     * 发送消息
     * <p>
     * send message.
     *
     * @param message
     * @param sent    already sent to socket?
     */
    void send(Object message, boolean sent) throws RemotingException;

    /**
     * close the channel.
     */
    void close();

    /**
     * Graceful close the channel.
     */
    void close(int timeout);

    /**
     * 关闭底层{@link Channel}
     */
    void startClose();

    /**
     * 检测底层{@link Channel}是否关闭
     * <p>
     * is closed.
     *
     * @return closed
     */
    boolean isClosed();

}