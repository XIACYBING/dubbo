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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelHandlers;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_CLIENT_THREADPOOL;
import static org.apache.dubbo.common.constants.CommonConstants.THREADPOOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREAD_NAME_KEY;

/**
 * 抽象客户端连接建立的相关逻辑，并留下相关{@code do*}方法让子类实现：
 *
 * @see #doOpen()
 * @see #doClose()
 * @see #doConnect()
 * @see #doDisConnect()
 * <p>
 * AbstractClient
 */
public abstract class AbstractClient extends AbstractEndpoint implements Client {

    protected static final String CLIENT_THREAD_POOL_NAME = "DubboClientHandler";
    private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);

    /**
     * 连接锁，在进行连接操作时要取锁才能操作
     */
    private final Lock connectLock = new ReentrantLock();

    /**
     * 发送数据前，检查Client底层连接是否断开，并根据{@link #needReconnect}判断是否进行重连
     */
    private final boolean needReconnect;

    /**
     * issue-7054:Consumer's executor is sharing globally.
     * <p>
     * 当前Client关联的线程池，所有消费者共享一个线程池
     */
    protected volatile ExecutorService executor;

    /**
     * 线程池管理仓库
     */
    private ExecutorRepository executorRepository =
        ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();

    public AbstractClient(URL url, ChannelHandler handler) throws RemotingException {

        // 调用父类构造方法，初始化codec、timeout、url和channelHandler相关内容
        super(url, handler);

        // 获取是否重连参数
        // set default needReconnect true when channel is not connected
        needReconnect = url.getParameter(Constants.SEND_RECONNECT_KEY, true);

        // 初始化线程池
        initExecutor(url);

        try {

            // 调用子类具体实现，启动当前Client
            doOpen();
        } catch (Throwable t) {
            close();
            throw new RemotingException(url.toInetSocketAddress(), null,
                    "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                            + " connect to the server " + getRemoteAddress() + ", cause: " + t.getMessage(), t);
        }

        try {

            // 创建连接，内部是通过子类的具体实现来创建连接的
            // connect.
            connect();
            if (logger.isInfoEnabled()) {
                logger.info(
                    "Start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress() + " connect to the server "
                        + getRemoteAddress());
            }
        } catch (RemotingException t) {
            if (url.getParameter(Constants.CHECK_KEY, true)) {
                close();
                throw t;
            } else {
                logger.warn("Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                        + " connect to the server " + getRemoteAddress() + " (check == false, ignore and retry later!), cause: " + t.getMessage(), t);
            }
        } catch (Throwable t) {
            close();
            throw new RemotingException(url.toInetSocketAddress(), null,
                    "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                            + " connect to the server " + getRemoteAddress() + ", cause: " + t.getMessage(), t);
        }
    }

    private void initExecutor(URL url) {

        // 所有消费URL共享同一个线程池
        //issue-7054:Consumer's executor is sharing globally, thread name not require provider ip.
        url = url.addParameter(THREAD_NAME_KEY, CLIENT_THREAD_POOL_NAME);
        url = url.addParameterIfAbsent(THREADPOOL_KEY, DEFAULT_CLIENT_THREADPOOL);
        executor = executorRepository.createExecutorIfAbsent(url);
    }

    protected static ChannelHandler wrapChannelHandler(URL url, ChannelHandler handler) {
        return ChannelHandlers.wrap(handler, url);
    }

    public InetSocketAddress getConnectAddress() {
        return new InetSocketAddress(NetUtils.filterLocalHost(getUrl().getHost()), getUrl().getPort());
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        Channel channel = getChannel();
        if (channel == null) {
            return getUrl().toInetSocketAddress();
        }
        return channel.getRemoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        Channel channel = getChannel();
        if (channel == null) {
            return InetSocketAddress.createUnresolved(NetUtils.getLocalHost(), 0);
        }
        return channel.getLocalAddress();
    }

    @Override
    public boolean isConnected() {
        Channel channel = getChannel();
        if (channel == null) {
            return false;
        }
        return channel.isConnected();
    }

    @Override
    public Object getAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return null;
        }
        return channel.getAttribute(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        Channel channel = getChannel();
        if (channel == null) {
            return;
        }
        channel.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return;
        }
        channel.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return false;
        }
        return channel.hasAttribute(key);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {

        // 连接断开，进行重连
        if (needReconnect && !isConnected()) {
            connect();
        }

        // 获取Channel：返回netty4的NettyChannel
        Channel channel = getChannel();

        // channel为空，或channel连接断开，则抛出异常
        //TODO Can the value returned by getChannel() be null? need improvement.
        if (channel == null || !channel.isConnected()) {
            throw new RemotingException(this, "message can not send, because channel is closed . url:" + getUrl());
        }

        // 通过channel发送数据：netty4的NettyChannel.send
        channel.send(message, sent);
    }

    protected void connect() throws RemotingException {
        connectLock.lock();

        try {
            if (isConnected()) {
                return;
            }

            if (isClosed() || isClosing()) {
                logger.warn("No need to connect to server " + getRemoteAddress() + " from " + getClass().getSimpleName() + " "
                        + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion() + ", cause: client status is closed or closing.");
                return;
            }

            doConnect();

            if (!isConnected()) {
                throw new RemotingException(this, "Failed connect to server " + getRemoteAddress() + " from " + getClass().getSimpleName() + " "
                                + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                                + ", cause: Connect wait timeout: " + getConnectTimeout() + "ms.");

            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Successed connect to server " + getRemoteAddress() + " from " + getClass().getSimpleName() + " "
                                    + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                                    + ", channel is " + this.getChannel());
                }
            }

        } catch (RemotingException e) {
            throw e;

        } catch (Throwable e) {
            throw new RemotingException(this, "Failed connect to server " + getRemoteAddress() + " from " + getClass().getSimpleName() + " "
                            + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                            + ", cause: " + e.getMessage(), e);

        } finally {
            connectLock.unlock();
        }
    }

    public void disconnect() {
        connectLock.lock();
        try {

            // 关闭通道
            try {
                Channel channel = getChannel();
                if (channel != null) {
                    channel.close();
                }
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }

            // 执行子类实现的具体断开连接的方法
            try {
                doDisConnect();
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void reconnect() throws RemotingException {
        if (!isConnected()) {
            connectLock.lock();
            try {
                if (!isConnected()) {
                    disconnect();
                    connect();
                }
            } finally {
                connectLock.unlock();
            }
        }
    }

    @Override
    public void close() {

        // 已关闭的不处理
        if (isClosed()) {
            logger.warn(
                "No need to close connection to server " + getRemoteAddress() + " from " + getClass().getSimpleName()
                    + " " + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                    + ", cause: the client status is closed.");
            return;
        }

        connectLock.lock();
        try {
            if (isClosed()) {
                logger.warn("No need to close connection to server " + getRemoteAddress() + " from "
                    + getClass().getSimpleName() + " " + NetUtils.getLocalHost() + " using dubbo version "
                    + Version.getVersion() + ", cause: the client status is closed.");
                return;
            }

            // 调用父类处理关闭事项：实际上是设置closed状态
            try {
                super.close();
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }

            // 断开连接
            try {
                disconnect();
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }

            // 执行子类具体的关闭逻辑
            try {
                doClose();
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }

        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close(int timeout) {
        close();
    }

    @Override
    public String toString() {
        return getClass().getName() + " [" + getLocalAddress() + " -> " + getRemoteAddress() + "]";
    }

    /**
     * Open client.
     *
     * @throws Throwable
     */
    protected abstract void doOpen() throws Throwable;

    /**
     * Close client.
     *
     * @throws Throwable
     */
    protected abstract void doClose() throws Throwable;

    /**
     * Connect to server.
     *
     * @throws Throwable
     */
    protected abstract void doConnect() throws Throwable;

    /**
     * disConnect to server.
     *
     * @throws Throwable
     */
    protected abstract void doDisConnect() throws Throwable;

    /**
     * Get the connected channel.
     *
     * @return channel
     */
    protected abstract Channel getChannel();
}
