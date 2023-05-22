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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;
import org.apache.dubbo.remoting.transport.DecodeHandler;
import org.apache.dubbo.remoting.transport.MultiMessageHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static org.apache.dubbo.common.constants.CommonConstants.READONLY_EVENT;

/**
 * {@link ExchangeHandler}的装饰器，相关核心逻辑由{@link #handler}实现
 * <p>
 * 请求的发起方一般是：{@link HeaderExchangeChannel#request}
 * <p>
 * 数据接收者，作为接收者，不管是请求或响应，都是由此开始进行处理
 * <p>
 * 处理请求：{@link #handleRequest(ExchangeChannel, Request)}
 * 处理响应：{@link #handleResponse(Channel, Response)}
 * 处理事件：{@link #handlerEvent(Channel, Request)}
 * <p>
 * 嵌套：{@link org.apache.dubbo.remoting.transport.netty4.NettyClient}/{@link org.apache.dubbo.remoting.transport.netty4.NettyServer}
 * -> {@link MultiMessageHandler} -> {@link HeartbeatHandler} -> {@link DecodeHandler} -> {@link HeaderExchangeHandler}
 * <p>
 * ExchangeReceiver
 */
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(HeaderExchangeHandler.class);

    /**
     * {@code exchange}层用来处理{@link Request}和{@link Response}的处理器
     *
     * @see #connected(Channel)
     * @see #disconnected(Channel)
     * @see #sent(Channel, Object)
     * @see #received(Channel, Object)
     * @see #caught(Channel, Throwable)
     */
    private final ExchangeHandler handler;

    public HeaderExchangeHandler(ExchangeHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.handler = handler;
    }

    static void handleResponse(Channel channel, Response response) throws RemotingException {
        if (response != null && !response.isHeartbeat()) {

            // 只设置Response的result数据到channel对应的DefaultFuture中
            DefaultFuture.received(channel, response);
        }
    }

    private static boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(url.getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    void handlerEvent(Channel channel, Request req) throws RemotingException {

        // 在Channel上设置只读标识：CHANNEL_ATTRIBUTE_READONLY_KEY，即channel.readonly
        // 后续上层会直接调用该值进行一些处理
        if (req.getData() != null && req.getData().equals(READONLY_EVENT)) {
            channel.setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        }
    }

    void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
        Response res = new Response(req.getId(), req.getVersion());

        // 如果请求解码异常，则根据直接不处理，返回BAD_REQUEST
        if (req.isBroken()) {
            Object data = req.getData();

            String msg;
            if (data == null) {
                msg = null;
            } else if (data instanceof Throwable) {
                msg = StringUtils.toString((Throwable)data);
            } else {
                msg = data.toString();
            }
            res.setErrorMessage("Fail to decode request due to: " + msg);
            res.setStatus(Response.BAD_REQUEST);

            // 发送响应
            channel.send(res);
            return;
        }

        // 获取Data，一般是RpcInvocation
        // find handler by message class.
        Object msg = req.getData();
        try {

            // 由上层实现的ExchangeHandler来处理请求
            // 比如DubboProtocol的内部属性requestHandler（ExchangeHandlerAdapter的实现类）
            CompletionStage<Object> future = handler.reply(channel, msg);

            // 附加响应处理完成后的逻辑：根据处理结果设置res，然后发送响应到consumer
            future.whenComplete((appResult, t) -> {
                try {

                    // 如果没有异常，则正常设置响应即可
                    if (t == null) {
                        res.setStatus(Response.OK);
                        res.setResult(appResult);
                    }

                    // 如果发现了异常，则设置异常信息，且设置响应状态为SERVICE_ERROR
                    else {
                        res.setStatus(Response.SERVICE_ERROR);
                        res.setErrorMessage(StringUtils.toString(t));
                    }

                    // 发送响应
                    channel.send(res);
                } catch (RemotingException e) {
                    logger.warn("Send result to consumer failed, channel is " + channel + ", msg is " + e);
                }
            });
        } catch (Throwable e) {
            res.setStatus(Response.SERVICE_ERROR);
            res.setErrorMessage(StringUtils.toString(e));
            channel.send(res);
        }
    }

    @Override
    public void connected(Channel channel) throws RemotingException {

        // 创建ExchangeChannel，并通知上层的handler
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        handler.connected(exchangeChannel);
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {

        // 获取exchangeChannel
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {

            // 通知上层连接断开
            handler.disconnected(exchangeChannel);
        } finally {

            // 关闭当前通道的所有请求
            DefaultFuture.closeChannel(channel);

            // 将HeaderExchangeChannel和底层的channel互相解绑
            HeaderExchangeChannel.removeChannel(channel);
        }
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        Throwable exception = null;
        try {
            ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
            handler.sent(exchangeChannel, message);
        } catch (Throwable t) {
            exception = t;
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }

        // 如果是请求，设置则对对应请求的DefaultFuture设置请求发送时间sent
        if (message instanceof Request) {
            Request request = (Request)message;
            DefaultFuture.sent(channel, request);
        }

        // 如果发送异常，根据异常类型进行处理
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException)exception;
            } else if (exception instanceof RemotingException) {
                throw (RemotingException)exception;
            } else {
                throw new RemotingException(channel.getLocalAddress(), channel.getRemoteAddress(),
                    exception.getMessage(), exception);
            }
        }
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        final ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);

        // 如果接收到请求
        if (message instanceof Request) {
            // handle request.
            Request request = (Request)message;

            // 事件请求的处理
            if (request.isEvent()) {
                handlerEvent(channel, request);
            }

            // 普通请求的处理
            else {

                // 需要发送响应（双向）
                if (request.isTwoWay()) {
                    handleRequest(exchangeChannel, request);
                }

                // 无需返回响应，则只是接收数据(单向)
                // 由上层实现的ExchangeHandler来处理请求
                // 比如DubboProtocol的内部属性requestHandler（ExchangeHandlerAdapter的实现类）

                // todo 单向请求如果解码失败，此处的data会是throwable类型，然后在reply方法中直接抛出异常，但是解码的异常会被丢弃掉，这是否合理，应该要让provider感知到为什么解码失败会比较合理吧？
                else {
                    handler.received(exchangeChannel, request.getData());
                }
            }
        }

        // 如果消息是Response，则根据Response中的id获取对应的DefaultFuture，并设置响应接口到Future中
        else if (message instanceof Response) {
            handleResponse(channel, (Response)message);
        }

        // 处理String请求，可能是telnet的echo请求
        else if (message instanceof String) {
            if (isClientSide(channel)) {
                Exception e = new Exception(
                    "Dubbo client can not supported string message: " + message + " in channel: " + channel + ", url: "
                        + channel.getUrl());
                logger.error(e.getMessage(), e);
            } else {
                String echo = handler.telnet(channel, (String)message);
                if (echo != null && echo.length() > 0) {
                    channel.send(echo);
                }
            }
        }

        // 不是以上几种，则交给handler处理
        else {
            handler.received(exchangeChannel, message);
        }
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                if (req.isTwoWay() && !req.isHeartbeat()) {
                    Response res = new Response(req.getId(), req.getVersion());
                    res.setStatus(Response.SERVER_ERROR);
                    res.setErrorMessage(StringUtils.toString(e));
                    channel.send(res);
                    return;
                }
            }
        }
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.caught(exchangeChannel, exception);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }
}
