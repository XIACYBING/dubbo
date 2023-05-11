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
package org.apache.dubbo.remoting.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.transport.AbstractChannel;
import org.apache.dubbo.remoting.utils.PayloadDropper;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * NettyChannel maintains the cache of channel.
 */
final class NettyChannel extends AbstractChannel {

    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class);

    /**
     * Netty的{@link Channel}和Dubbo的{@link NettyChannel}的映射，全局共享
     * <p>
     * the cache for netty channel and dubbo channel
     */
    private static final ConcurrentMap<Channel, NettyChannel> CHANNEL_MAP =
        new ConcurrentHashMap<Channel, NettyChannel>();

    /**
     * 当前关联的Netty的Channel
     * <p>
     * netty channel
     */
    private final Channel channel;

    /**
     * Channel上关联的属性，由以下方法维护：
     *
     * @see #hasAttribute(String)
     * @see #getAttribute(String)
     * @see #setAttribute(String, Object)
     * @see #close()
     */
    private final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    /**
     * 当前Channel是否存活的标识
     */
    private final AtomicBoolean active = new AtomicBoolean(false);

    /**
     * The constructor of NettyChannel.
     * It is private so NettyChannel usually create by {@link NettyChannel#getOrAddChannel(Channel, URL, ChannelHandler)}
     *
     * @param channel netty channel
     * @param url
     * @param handler dubbo handler that contain netty handler
     */
    private NettyChannel(Channel channel, URL url, ChannelHandler handler) {
        super(url, handler);
        if (channel == null) {
            throw new IllegalArgumentException("netty channel == null;");
        }
        this.channel = channel;
    }

    /**
     * Get dubbo channel by netty channel through channel cache.
     * Put netty channel into it if dubbo channel don't exist in the cache.
     *
     * @param ch      netty channel
     * @param url
     * @param handler dubbo handler that contain netty's handler
     * @return
     */
    static NettyChannel getOrAddChannel(Channel ch, URL url, ChannelHandler handler) {
        if (ch == null) {
            return null;
        }
        NettyChannel ret = CHANNEL_MAP.get(ch);
        if (ret == null) {
            NettyChannel nettyChannel = new NettyChannel(ch, url, handler);
            if (ch.isActive()) {
                nettyChannel.markActive(true);
                ret = CHANNEL_MAP.putIfAbsent(ch, nettyChannel);
            }
            if (ret == null) {
                ret = nettyChannel;
            }
        }
        return ret;
    }

    /**
     * Remove the inactive channel.
     *
     * @param ch netty channel
     */
    static void removeChannelIfDisconnected(Channel ch) {
        if (ch != null && !ch.isActive()) {
            NettyChannel nettyChannel = CHANNEL_MAP.remove(ch);
            if (nettyChannel != null) {
                nettyChannel.markActive(false);
            }
        }
    }

    static void removeChannel(Channel ch) {
        if (ch != null) {
            NettyChannel nettyChannel = CHANNEL_MAP.remove(ch);
            if (nettyChannel != null) {
                nettyChannel.markActive(false);
            }
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public boolean isConnected() {
        return !isClosed() && active.get();
    }

    public boolean isActive() {
        return active.get();
    }

    public void markActive(boolean isActive) {
        active.set(isActive);
    }

    /**
     * Send message by netty and whether to wait the completion of the send.
     *
     * @param message message that need send.
     * @param sent    whether to ack async-sent
     * @throws RemotingException throw RemotingException if wait until timeout or any exception thrown by method body that surrounded by try-catch.
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {

        // 检查channel是否关闭
        // whether the channel is closed
        super.send(message, sent);

        boolean success = true;
        int timeout = 0;
        try {

            // 调用netty接口，写入并刷新数据：NioSocketChannel.writeAndFlush
            // ChannelFuture不需要对外返回，可以通过其他方法从channel中获取服务提供者的响应数据
            ChannelFuture future = channel.writeAndFlush(message);

            // 如果sent为true，则需要超时等待服务提供者的响应，相当于同步获取响应
            if (sent) {

                // 超时毫秒数，默认值为1000
                // wait timeout ms
                timeout = getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);

                // 等待响应，如果超时时间内提供者返回响应，则返回true，否则返回false
                success = future.await(timeout);
            }

            // 获取异常，不为空则直接对外抛出
            Throwable cause = future.cause();
            if (cause != null) {
                throw cause;
            }
        } catch (Throwable e) {

            // 如果连接断开，则移除当前channel
            removeChannelIfDisconnected(channel);

            // 包装异常
            throw new RemotingException(this,
                "Failed to send message " + PayloadDropper.getRequestWithoutData(message) + " to " + getRemoteAddress()
                    + ", cause: " + e.getMessage(), e);
        }

        // 请求失败时，抛出异常；
        // sent为false时，不需要等待服务端响应回来，可以先返回给外围，此时success为true
        if (!success) {
            throw new RemotingException(this,
                "Failed to send message " + PayloadDropper.getRequestWithoutData(message) + " to " + getRemoteAddress()
                    + "in timeout(" + timeout + "ms) limit");
        }
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            removeChannelIfDisconnected(channel);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            attributes.clear();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close netty channel " + channel);
            }
            channel.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        // The null value is unallowed in the ConcurrentHashMap.
        if (value == null) {
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    @Override
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        // FIXME: a hack to make org.apache.dubbo.remoting.exchange.support.DefaultFuture.closeChannel work
        if (obj instanceof NettyClient) {
            NettyClient client = (NettyClient) obj;
            return channel.equals(client.getNettyChannel());
        }

        if (getClass() != obj.getClass()) {
            return false;
        }
        NettyChannel other = (NettyChannel) obj;
        if (channel == null) {
            if (other.channel != null) {
                return false;
            }
        } else if (!channel.equals(other.channel)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "NettyChannel [channel=" + channel + "]";
    }

}
