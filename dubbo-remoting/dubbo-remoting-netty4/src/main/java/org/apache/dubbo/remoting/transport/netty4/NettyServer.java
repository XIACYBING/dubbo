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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.RemotingServer;
import org.apache.dubbo.remoting.transport.AbstractServer;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelHandlers;
import org.apache.dubbo.remoting.utils.UrlUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.dubbo.common.constants.CommonConstants.IO_THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.KEEP_ALIVE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SSL_ENABLED_KEY;


/**
 * NettyServer.
 */
public class NettyServer extends AbstractServer implements RemotingServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    /**
     * 客户端连接的Channel映射集合
     * <p>
     * the cache for alive worker channel.
     * <ip:port, dubbo channel>
     */
    private Map<String, Channel> channels;

    /**
     * 代表Netty服务器的Bootstrap
     * <p>
     * netty server bootstrap.
     */
    private ServerBootstrap bootstrap;

    /**
     * 绑定端口的服务器Channel
     *
     * the boss channel that receive connections and dispatch these to worker channel.
     */
    private io.netty.channel.Channel channel;

    /**
     * 客户端连接处理线程组（线程池）
     */
    private EventLoopGroup bossGroup;

    /**
     * 客户端工作处理线程组（线程池）
     */
    private EventLoopGroup workerGroup;

    public NettyServer(URL url, ChannelHandler handler) throws RemotingException {
        // you can customize name and type of client thread pool by THREAD_NAME_KEY and THREADPOOL_KEY in CommonConstants.
        // the handler will be wrapped: MultiMessageHandler->HeartbeatHandler->handler

        // 在HeaderExchanger -> NettyTransporter -> NettyServer的链路中，最终保存在NettyServer中的ChannelHandler的层级如下：
        // MultiMessageHandler -> HeartbeatHandler -> AllChannelHandler -> DecodeHandler -> HeaderExchangeHandler ->
        // handler（HeaderExchanger.bind入参的handler）
        super(ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME), ChannelHandlers.wrap(handler, url));
    }

    /**
     * Init and start netty server
     *
     * @throws Throwable
     */
    @Override
    protected void doOpen() throws Throwable {

        // 创建代表服务器的ServerBootstrap
        bootstrap = new ServerBootstrap();

        // 生成Reactor线程组，监听连接：此处只使用一个线程来监听
        bossGroup = NettyEventLoopFactory.eventLoopGroup(1, "NettyServerBoss");

        // 生成工作线程组，处理连接的IO读写和业务处理
        workerGroup = NettyEventLoopFactory.eventLoopGroup(
            getUrl().getPositiveParameter(IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS), "NettyServerWorker");

        // 创建NettyServerHandler，它是ChannelDuplexHandler的一个实现 todo 似乎是用来进行连接，读取和写入数据的一个处理器
        final NettyServerHandler nettyServerHandler = new NettyServerHandler(getUrl(), this);

        // 获取存储channel的集合
        channels = nettyServerHandler.getChannels();

        boolean keepalive = getUrl().getParameter(KEEP_ALIVE_KEY, Boolean.FALSE);

        // 初始化服务器bootstrap
        bootstrap
            .group(bossGroup, workerGroup)
            .channel(NettyEventLoopFactory.serverSocketChannelClass())
            .option(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
            .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .childOption(ChannelOption.SO_KEEPALIVE, keepalive)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

            // 设置通道初始化器：初始化客户端通道时使用
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {

                    // 获取空闲时间
                    // FIXME: should we use getTimeout()?
                    int idleTimeout = UrlUtils.getIdleTimeout(getUrl());

                    // 生成编码适配器，底层都是通过调用getCodec传入的对象去处理的
                    NettyCodecAdapter adapter = new NettyCodecAdapter(getCodec(), getUrl(), NettyServer.this);
                    if (getUrl().getParameter(SSL_ENABLED_KEY, false)) {
                        ch
                            .pipeline()
                            .addLast("negotiation",
                                SslHandlerInitializer.sslServerHandler(getUrl(), nettyServerHandler));
                    }

                    // 在Netty的IO线程中执行的处理器有：InternalDecode、InternalEncoder、IdleStateHandler、NettyServerHandler
                    // （NettyServerHandler中的ChannelHandler操作会委托给NettyServer，最终委托给NettyServer中的Handler）
                    ch.pipeline()

                      // 注册Decoder和Encoder：内部对象，会调用adapter的codec（实现类一般是DubboCodec）属性去处理相关编码任务
                      .addLast("decoder", adapter.getDecoder()).addLast("encoder", adapter.getEncoder())

                      // 服务器空闲处理器，Netty提供的一个工具型的ChannelHandler，用于定时心跳请求，或关闭长时间空闲连接的功能
                      // IdleStateHandler会通过lastReadTime、lastWriteTime等几个字段，记录最近一次读/写的事件的时间，
                      // IdleStateHandler初始化的时候，会创建一个定时任务，定时检测当前时间与最后一次读/写时间的差值，
                      // 如果超过我们设置的阈值（也就是构造器中设置的idleTimeout），就会触发IdleStateEvent 事件，并传递给后续的ChannelHandler 进行处理，
                      // 后续ChannelHandler的userEventTriggered()方法会根据接收到的 IdleStateEvent 事件，决定是关闭长时间空闲的连接，还是发送心跳探活。
                      .addLast("server-idle-handler", new IdleStateHandler(0, 0, idleTimeout, MILLISECONDS))
                      .addLast("handler", nettyServerHandler);
                }
            });

        // 绑定地址和端口
        // bind
        ChannelFuture channelFuture = bootstrap.bind(getBindAddress());

        // 等待绑定完成
        channelFuture.syncUninterruptibly();

        // 获取代表服务器的channel
        channel = channelFuture.channel();

    }

    @Override
    protected void doClose() throws Throwable {

        // 关闭服务端的NettyChannel
        try {
            if (channel != null) {
                // unbind.
                channel.close();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }

        // 关闭代表客户端连接的Channel
        try {
            Collection<Channel> channels = getChannels();
            if (channels != null && channels.size() > 0) {
                for (Channel channel : channels) {
                    try {
                        channel.close();
                    } catch (Throwable e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }

        // 关闭Netty相关的线程池
        try {
            if (bootstrap != null) {
                bossGroup.shutdownGracefully().syncUninterruptibly();
                workerGroup.shutdownGracefully().syncUninterruptibly();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }

        // 清理所有客户端Channel
        try {
            if (channels != null) {
                channels.clear();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public Collection<Channel> getChannels() {
        Collection<Channel> chs = new ArrayList<>(this.channels.size());
        chs.addAll(this.channels.values());
        // check of connection status is unnecessary since we are using channels in NettyServerHandler
//        for (Channel channel : this.channels.values()) {
//            if (channel.isConnected()) {
//                chs.add(channel);
//            } else {
//                channels.remove(NetUtils.toAddressString(channel.getRemoteAddress()));
//            }
//        }
        return chs;
    }

    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return channels.get(NetUtils.toAddressString(remoteAddress));
    }

    @Override
    public boolean canHandleIdle() {
        return true;
    }

    @Override
    public boolean isBound() {
        return channel.isActive();
    }

}
