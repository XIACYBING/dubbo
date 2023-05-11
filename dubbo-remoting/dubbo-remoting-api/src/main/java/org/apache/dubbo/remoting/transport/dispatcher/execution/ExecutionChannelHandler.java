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
package org.apache.dubbo.remoting.transport.dispatcher.execution;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelEventRunnable;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelEventRunnable.ChannelState;
import org.apache.dubbo.remoting.transport.dispatcher.WrappedChannelHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * 只有请求消息会被调度给线程池处理，其他方法还是由相关的IO线程处理（比如Netty的IO线程）
 * <p>
 * 响应消息关联的如果是{@link ThreadlessExecutor}，也会被提交到线程池中处理
 * <p>
 * Only request message will be dispatched to thread pool. Other messages like response, connect, disconnect,
 * heartbeat will be directly executed by I/O thread.
 */
public class ExecutionChannelHandler extends WrappedChannelHandler {

    public ExecutionChannelHandler(ChannelHandler handler, URL url) {
        super(handler, url);
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {

        // 获取线程池
        ExecutorService executor = getPreferredExecutorService(message);

        // 如果是请求，则提交给线程池
        if (message instanceof Request) {
            try {
                executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.RECEIVED, message));
            } catch (Throwable t) {
                // FIXME: when the thread pool is full, SERVER_THREADPOOL_EXHAUSTED_ERROR cannot return properly,
                // therefore the consumer side has to wait until gets timeout. This is a temporary solution to prevent
                // this scenario from happening, but a better solution should be considered later.
                if (t instanceof RejectedExecutionException) {
                    sendFeedback(channel, (Request)message, t);
                }
                throw new ExecutionException(message, channel, getClass() + " error when process received event.", t);
            }
        }

        // 如果线程池是ThreadlessExecutor，一般代表当前message是响应，提交给线程池处理就行
        else if (executor instanceof ThreadlessExecutor) {
            executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.RECEIVED, message));
        }

        // 否则使用当前线程处理即可：一般是网络框架的IO线程
        else {
            handler.received(channel, message);
        }
    }
}
