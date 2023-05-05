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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.remoting.transport.ChannelHandlerAdapter;
import org.apache.dubbo.remoting.transport.ChannelHandlerDispatcher;

/**
 * Transporter门面类，基于{@link Transporter}的适配器实现，提供{@link Client}和{@link RemotingServer}创建的能力
 * <p>
 * Transporter facade. (API, Static, ThreadSafe)
 */
public class Transporters {

    static {
        // check duplicate jar package
        Version.checkDuplicate(Transporters.class);
        Version.checkDuplicate(RemotingException.class);
    }

    private Transporters() {
    }

    public static RemotingServer bind(String url, ChannelHandler... handler) throws RemotingException {
        return bind(URL.valueOf(url), handler);
    }

    /**
     * 生成一个服务器，具体逻辑由具体的{@link org.apache.dubbo.rpc.Protocol}实现类维护
     * 一般是一个IP一个服务器，并缓存在{@link org.apache.dubbo.rpc.protocol.AbstractProtocol#serverMap}
     *
     * @param url      创建服务器的URL，一般是某个提供者URL
     * @param handlers 通道处理器
     * @return 返回创建完成的服务器
     */
    public static RemotingServer bind(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("handlers == null");
        }
        ChannelHandler handler;
        if (handlers.length == 1) {
            handler = handlers[0];
        } else {

            // 生成ChannelHandler调度器
            handler = new ChannelHandlerDispatcher(handlers);
        }
        return getTransporter().bind(url, handler);
    }

    public static Client connect(String url, ChannelHandler... handler) throws RemotingException {
        return connect(URL.valueOf(url), handler);
    }

    /**
     * 连接到一个远程服务器，并返回相应的{@link Client}
     * <p>
     * 比如A系统消费B系统的三个接口，消费C系统的两个接口，那么此时A系统就需要通过{@link #connect}创建两个{@link Client}，分别连接B系统和C系统
     *
     * @param url      相应的url，主要用到url上的IP地址和相关参数
     * @param handlers 通道处理器数组
     * @return 返回创建完成的 {@link Client}
     */
    public static Client connect(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        ChannelHandler handler;
        if (handlers == null || handlers.length == 0) {
            handler = new ChannelHandlerAdapter();
        } else if (handlers.length == 1) {
            handler = handlers[0];
        } else {

            // 生成ChannelHandler调度器
            handler = new ChannelHandlerDispatcher(handlers);
        }
        return getTransporter().connect(url, handler);
    }

    /**
     * 获取Transporter的适配器类，适配器类会根据外部url的相关参数，和{@link org.apache.dubbo.common.extension.SPI}注解的默认值进行处理
     */
    public static Transporter getTransporter() {
        return ExtensionLoader.getExtensionLoader(Transporter.class).getAdaptiveExtension();
    }

}