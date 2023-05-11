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
package org.apache.dubbo.remoting.transport.dispatcher;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;
import org.apache.dubbo.remoting.transport.dispatcher.all.AllChannelHandler;
import org.apache.dubbo.remoting.transport.dispatcher.connection.ConnectionOrderedChannelHandler;
import org.apache.dubbo.remoting.transport.dispatcher.direct.DirectChannelHandler;
import org.apache.dubbo.remoting.transport.dispatcher.execution.ExecutionChannelHandler;
import org.apache.dubbo.remoting.transport.dispatcher.message.MessageOnlyChannelHandler;

import java.util.concurrent.ExecutorService;

/**
 * {@link ChannelHandler}的包装类，实际的处理逻辑还是交给作为构造器参数传入的{@link #handler}，只是对其进行一些逻辑封装，比如{@link AllChannelHandler}会将处理提交到线程池中
 * <p>
 * 当前类的不同实现会进行不同的处理：主要体现在某些逻辑会提交给线程池处理，其他的逻辑会直接交给调用的IO线程处理
 * {@link AllChannelHandler}会将除{@link #sent}外的其他方法提交给线程池处理
 * {@link MessageOnlyChannelHandler}会将{@link #received}接收到的所有消息都提交给线程池处理
 * {@link ExecutionChannelHandler}则只会在{@link #received}中将请求消息/有关联{@link ThreadlessExecutor}的响应消息提交给线程池处理
 * {@link DirectChannelHandler}则只会将{@link #received}中有关联{@link ThreadlessExecutor}的响应消息提交给线程池处理
 * {@link ConnectionOrderedChannelHandler}则会将{@link #received}和{@link #caught}方法提交给获取到的线程池处理，而且还在内部维护
 * 一个线程池{@link ConnectionOrderedChannelHandler#connectionExecutor}来处理连接相关的任务{@link #connected}和{@link #disconnected}
 */
public class WrappedChannelHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(WrappedChannelHandler.class);

    protected final ChannelHandler handler;

    protected final URL url;

    public WrappedChannelHandler(ChannelHandler handler, URL url) {
        this.handler = handler;
        this.url = url;
    }

    public void close() {

    }

    @Override
    public void connected(Channel channel) throws RemotingException {
        handler.connected(channel);
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        handler.disconnected(channel);
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        handler.sent(channel, message);
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        handler.received(channel, message);
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        handler.caught(channel, exception);
    }

    protected void sendFeedback(Channel channel, Request request, Throwable t) throws RemotingException {
        if (request.isTwoWay()) {
            String msg = "Server side(" + url.getIp() + "," + url.getPort()
                    + ") thread pool is exhausted, detail msg:" + t.getMessage();
            Response response = new Response(request.getId(), request.getVersion());
            response.setStatus(Response.SERVER_THREADPOOL_EXHAUSTED_ERROR);
            response.setErrorMessage(msg);
            channel.send(response);
            return;
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

    public URL getUrl() {
        return url;
    }

    /**
     * 如果{@code msg}是一个响应，且有对应的{@link DefaultFuture}，且{@link DefaultFuture#executor}是一个有效线程池，则使用该线程池；
     * 否则使用{@link #getSharedExecutorService()}返回的共享线程池
     * <p>
     * Currently, this method is mainly customized to facilitate the thread model on consumer side.
     * 1. Use ThreadlessExecutor, aka., delegate callback directly to the thread initiating the call.
     * 2. Use shared executor to execute the callback.
     * <p>
     * 如果返回的线程池服务是{@link ThreadlessExecutor}，则当前只是将{@link ChannelEventRunnable}任务提交到线程池中，由发起请求的线程通过调用
     * {@link ThreadlessExecutor#waitAndDrain()}方法，执行响应处理任务，并获取响应结果
     * <p>
     * 如果非{@link ThreadlessExecutor}，则只是将任务提交给线程池执行处理
     */
    public ExecutorService getPreferredExecutorService(Object msg) {
        if (msg instanceof Response) {
            Response response = (Response)msg;

            // 获取响应对应的Future
            DefaultFuture responseFuture = DefaultFuture.getFuture(response.getId());

            // 如果Future为空，则获取共享线程池：比如超时响应场景，Future会被超时任务处理掉，此时根据id获取到的Future就会为空
            // a typical scenario is the response returned after timeout, the timeout response may has completed the future
            if (responseFuture == null) {
                return getSharedExecutorService();
            } else {

                // Future不为空，获取关联的线程池
                ExecutorService executor = responseFuture.getExecutor();

                // 关联线程池为空，或关联线程池已关闭，则直接使用共享线程池
                if (executor == null || executor.isShutdown()) {
                    executor = getSharedExecutorService();
                }

                // 否则返回Future关联的线程池
                return executor;
            }
        }

        // 非Response的消息，则直接使用共享线程池处理
        else {
            return getSharedExecutorService();
        }
    }

    /**
     * get the shared executor for current Server or Client
     *
     * @return
     */
    public ExecutorService getSharedExecutorService() {

        // 获取默认的线程池仓库
        ExecutorRepository executorRepository =
            ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();

        // 根据URL获取线程池，如果没有则创建
        ExecutorService executor = executorRepository.getExecutor(url);
        if (executor == null) {
            executor = executorRepository.createExecutorIfAbsent(url);
        }
        return executor;
    }

    @Deprecated
    public ExecutorService getExecutorService() {
        return getSharedExecutorService();
    }


}
