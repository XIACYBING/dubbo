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
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.remoting.transport.dispatcher.WrappedChannelHandler;
import org.apache.dubbo.remoting.transport.dispatcher.all.AllChannelHandler;
import org.apache.dubbo.remoting.transport.dispatcher.all.AllDispatcher;

/**
 * 消息派发器，派发消息到某个线程池中，相关子类返回的是{@link WrappedChannelHandler}的实现类
 * <p>
 * 默认是：{@link AllDispatcher}，返回的结果是{@link AllChannelHandler}
 * <p>
 * {@link Dispatcher}会将入参中的{@link ChannelHandler}包装到{@link WrappedChannelHandler}中，并进行相关的线程池处理
 * <p>
 * ChannelHandlerWrapper (SPI, Singleton, ThreadSafe)
 */
@SPI(AllDispatcher.NAME)
public interface Dispatcher {

    /**
     * dispatch the message to threadpool.
     * <p>
     * The last two parameters are reserved for compatibility with the old configuration
     *
     * @param handler 实际进行处理的ChannelHandler
     * @param url     消费者或提供者URL
     * @return 返回对{@code handler}进行包装后的{@link WrappedChannelHandler}，并对{@link ChannelHandler}的相关处理方法用线程池封装
     */
    @Adaptive({Constants.DISPATCHER_KEY, "dispather", "channel.handler"})
    ChannelHandler dispatch(ChannelHandler handler, URL url);

}
