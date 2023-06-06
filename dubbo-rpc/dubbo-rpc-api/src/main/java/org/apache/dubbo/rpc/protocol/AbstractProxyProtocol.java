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

package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.Parameters;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.RemotingServer;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;

/**
 * 抽象代理协议的通用实现，Dubbo协议底层通过Netty或其他网络传输框架进行数据交互，但是也支持其他交互，比如Grpc、Http等
 * <p>
 * 这些第三方的网络传输协议，需要通过代理去包装，比如Http调用，通过抽象出{@link org.apache.dubbo.remoting.http.HttpServer}作为服务器的抽象，从而进行数据交互
 * <p>
 * AbstractProxyProtocol
 */
public abstract class AbstractProxyProtocol extends AbstractProtocol {

    private final List<Class<?>> rpcExceptions = new CopyOnWriteArrayList<Class<?>>();

    protected ProxyFactory proxyFactory;

    public AbstractProxyProtocol() {
    }

    public AbstractProxyProtocol(Class<?>... exceptions) {
        for (Class<?> exception : exceptions) {
            addRpcException(exception);
        }
    }

    public void addRpcException(Class<?> exception) {
        this.rpcExceptions.add(exception);
    }

    public ProxyFactory getProxyFactory() {
        return proxyFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Exporter<T> export(final Invoker<T> invoker) throws RpcException {

        // 生成serviceKey
        final String uri = serviceKey(invoker.getUrl());

        // 根据serviceKey获取缓存
        Exporter<T> exporter = (Exporter<T>)exporterMap.getExport(uri);

        // 缓存不为空，且url一致，则可以直接返回
        if (exporter != null) {

            // 如果url不一致，则需要重新export
            // When modifying the configuration through override, you need to re-expose the newly modified service.
            if (Objects.equals(exporter.getInvoker().getUrl(), invoker.getUrl())) {
                return exporter;
            }
        }

        // 通过proxyFactory生成invoker的代理对象，并调用doExport执行export逻辑，并获取一个unExport之后的回调，用于销毁底层的ProtocolServer
        final Runnable runnable =
            doExport(proxyFactory.getProxy(invoker, true), invoker.getInterface(), invoker.getUrl());

        // 生成exporter，并实现afterUnExport逻辑
        exporter = new AbstractExporter<T>(invoker) {
            @Override
            public void afterUnExport() {
                exporterMap.removeExportMap(uri, this);
                if (runnable != null) {
                    try {
                        runnable.run();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            }
        };

        // 添加export到缓存中
        exporterMap.addExportMap(uri, exporter);
        return exporter;
    }

    @Override
    protected <T> Invoker<T> protocolBindingRefer(final Class<T> type, final URL url) throws RpcException {

        // 执行引用，并包装成Invoker
        final Invoker<T> target = proxyFactory.getInvoker(doRefer(type, url), type, url);

        // 基于target实现AbstractInvoker
        Invoker<T> invoker = new AbstractInvoker<T>(type, url) {
            @Override
            protected Result doInvoke(Invocation invocation) throws Throwable {
                try {
                    Result result = target.invoke(invocation);
                    // FIXME result is an AsyncRpcResult instance.
                    Throwable e = result.getException();
                    if (e != null) {
                        for (Class<?> rpcException : rpcExceptions) {
                            if (rpcException.isAssignableFrom(e.getClass())) {
                                throw getRpcException(type, url, invocation, e);
                            }
                        }
                    }
                    return result;
                } catch (RpcException e) {
                    if (e.getCode() == RpcException.UNKNOWN_EXCEPTION) {
                        e.setCode(getErrorCode(e.getCause()));
                    }
                    throw e;
                } catch (Throwable e) {
                    throw getRpcException(type, url, invocation, e);
                }
            }

            @Override
            public void destroy() {
                super.destroy();
                target.destroy();
                invokers.remove(this);
            }
        };

        // 添加到invoker集合中
        invokers.add(invoker);

        // 返回当前invoker
        return invoker;
    }

    protected RpcException getRpcException(Class<?> type, URL url, Invocation invocation, Throwable e) {
        RpcException re = new RpcException(
            "Failed to invoke remote service: " + type + ", method: " + invocation.getMethodName() + ", cause: "
                + e.getMessage(), e);
        re.setCode(getErrorCode(e));
        return re;
    }

    /**
     * 从url中获取地址信息
     *
     * @param url url
     * @return ip:port
     */
    protected String getAddr(URL url) {
        String bindIp = url.getParameter(Constants.BIND_IP_KEY, url.getHost());
        if (url.getParameter(ANYHOST_KEY, false)) {
            bindIp = ANYHOST_VALUE;
        }
        return NetUtils.getIpByHost(bindIp) + ":" + url.getParameter(Constants.BIND_PORT_KEY, url.getPort());
    }

    protected int getErrorCode(Throwable e) {
        return RpcException.UNKNOWN_EXCEPTION;
    }

    /**
     * 执行export逻辑
     *
     * @param impl invoker
     * @param type invoker具体的类型
     * @param url  url
     * @param <T>  泛型类型
     * @return 返回unExport之后的回调任务
     * @throws RpcException RpcException
     */
    protected abstract <T> Runnable doExport(T impl, Class<T> type, URL url) throws RpcException;

    protected abstract <T> T doRefer(Class<T> type, URL url) throws RpcException;

    protected class ProxyProtocolServer implements ProtocolServer {

        private RemotingServer server;
        private String address;

        public ProxyProtocolServer(RemotingServer server) {
            this.server = server;
        }

        @Override
        public RemotingServer getRemotingServer() {
            return server;
        }

        @Override
        public String getAddress() {
            return StringUtils.isNotEmpty(address) ? address : server.getUrl().getAddress();
        }

        @Override
        public void setAddress(String address) {
            this.address = address;
        }

        @Override
        public URL getUrl() {
            return server.getUrl();
        }

        @Override
        public void close() {
            server.close();
        }
    }

    protected abstract class RemotingServerAdapter implements RemotingServer {

        public abstract Object getDelegateServer();

        /**
         * @return
         */
        @Override
        public boolean isBound() {
            return false;
        }

        @Override
        public Collection<Channel> getChannels() {
            return null;
        }

        @Override
        public Channel getChannel(InetSocketAddress remoteAddress) {
            return null;
        }

        @Override
        public void reset(Parameters parameters) {

        }

        @Override
        public void reset(URL url) {

        }

        @Override
        public URL getUrl() {
            return null;
        }

        @Override
        public ChannelHandler getChannelHandler() {
            return null;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public void send(Object message) throws RemotingException {

        }

        @Override
        public void send(Object message, boolean sent) throws RemotingException {

        }

        @Override
        public void close() {

        }

        @Override
        public void close(int timeout) {

        }

        @Override
        public void startClose() {

        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }

}
