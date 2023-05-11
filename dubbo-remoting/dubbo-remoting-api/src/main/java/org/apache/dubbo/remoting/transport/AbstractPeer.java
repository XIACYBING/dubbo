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
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.Endpoint;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.support.header.HeaderExchangeHandler;
import org.apache.dubbo.remoting.exchange.support.header.HeaderExchanger;
import org.apache.dubbo.remoting.exchange.support.header.HeartbeatHandler;
import org.apache.dubbo.remoting.transport.dispatcher.all.AllChannelHandler;

/**
 * 对{@link Endpoint}的部分方法提供一些默认抽象实现（主要是通过{@link #closing}和{@link #closed}管理端点状态），
 * 以及将{@link ChannelHandler}的实现方法委托给实例常量{@link #handler}
 * <p>
 * AbstractPeer
 */
public abstract class AbstractPeer implements Endpoint, ChannelHandler {

    /**
     * 当前类也继承了{@link ChannelHandler}，但是自己并不进行相关处理，而是全部委托给{@link #handler}来进行处理
     * <p>
     * 继承当前抽象类的，都需要一个{@link ChannelHandler}来进行相关处理
     * <p>
     * 在{@link HeaderExchanger} -> {@link org.apache.dubbo.remoting.transport.netty4.NettyTransporter} ->
     * {@link org.apache.dubbo.remoting.transport.netty4.NettyServer}的链路中，最终保存在{@link org.apache.dubbo.remoting.transport.netty4.NettyServer}中的
     * {@link ChannelHandler}（即当前{@link #handler}）的层级如下：
     * <p>
     * {@link MultiMessageHandler} -> {@link HeartbeatHandler} -> {@link AllChannelHandler} -> {@link DecodeHandler} ->
     * {@link HeaderExchangeHandler} -> handler（{@link HeaderExchanger#bind}入参的handler）
     */
    private final ChannelHandler handler;

    /**
     * 表示当前端点自身的URL
     */
    private volatile URL url;

    /**
     * closing and closed means the process is being closed and close is finished
     * <p>
     * 为{@code true}代表当前端点正在关闭中
     */
    private volatile boolean closing;

    /**
     * 为{@code true}代表当前端点已关闭
     */
    private volatile boolean closed;

    public AbstractPeer(URL url, ChannelHandler handler) {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.url = url;
        this.handler = handler;
    }

    @Override
    public void send(Object message) throws RemotingException {

        // 发送数据，message是包装过的request，其中的data是invocation
        // 实际调用的是子类的AbstractClient.send，当前类的实例类对象是netty4的NettyClient
        // 如果sent为true，则需要在请求发起后等待结果响应，超时时间内没有响应，则抛出超时异常
        send(message, url.getParameter(Constants.SENT_KEY, false));
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void close(int timeout) {
        close();
    }

    @Override
    public void startClose() {
        if (isClosed()) {
            return;
        }
        closing = true;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        this.url = url;
    }

    @Override
    public ChannelHandler getChannelHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }

    /**
     * @return ChannelHandler
     */
    @Deprecated
    public ChannelHandler getHandler() {
        return getDelegateHandler();
    }

    /**
     * Return the final handler (which may have been wrapped). This method should be distinguished with getChannelHandler() method
     *
     * @return ChannelHandler
     */
    public ChannelHandler getDelegateHandler() {
        return handler;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public boolean isClosing() {
        return closing && !closed;
    }

    @Override
    public void connected(Channel ch) throws RemotingException {
        if (closed) {
            return;
        }
        handler.connected(ch);
    }

    @Override
    public void disconnected(Channel ch) throws RemotingException {
        handler.disconnected(ch);
    }

    @Override
    public void sent(Channel ch, Object msg) throws RemotingException {
        if (closed) {
            return;
        }
        handler.sent(ch, msg);
    }

    @Override
    public void received(Channel ch, Object msg) throws RemotingException {
        if (closed) {
            return;
        }
        handler.received(ch, msg);
    }

    @Override
    public void caught(Channel ch, Throwable ex) throws RemotingException {
        handler.caught(ch, ex);
    }
}
