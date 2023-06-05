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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncContextImpl;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * {@link Invoker}的抽象实现，在provider中，一般用于{@link org.apache.dubbo.rpc.ProxyFactory}中生成实际业务类的{@link Invoker}包装来使用
 * <p>
 * This Invoker works on provider side, delegates RPC to interface implementation.
 */
public abstract class AbstractProxyInvoker<T> implements Invoker<T> {
    Logger logger = LoggerFactory.getLogger(AbstractProxyInvoker.class);

    /**
     * 被包装的代理对象
     */
    private final T proxy;

    private final Class<T> type;

    private final URL url;

    public AbstractProxyInvoker(T proxy, Class<T> type, URL url) {
        if (proxy == null) {
            throw new IllegalArgumentException("proxy == null");
        }
        if (type == null) {
            throw new IllegalArgumentException("interface == null");
        }
        if (!type.isInstance(proxy)) {
            throw new IllegalArgumentException(proxy.getClass().getName() + " not implement interface " + type);
        }
        this.proxy = proxy;
        this.type = type;
        this.url = url;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        try {

            // 调用实际的方法，获取结果：doInvoke由子类自行实现，需要实现具体的方法调用逻辑
            Object value =
                doInvoke(proxy, invocation.getMethodName(), invocation.getParameterTypes(), invocation.getArguments());

            // 包装value为CompletableFuture
            CompletableFuture<Object> future = wrapWithFuture(value);

            // 为future添加处理逻辑，将future转换为CompletableFuture<AppResponse>
            CompletableFuture<AppResponse> appResponseFuture = future.handle((obj, t) -> {

                // 生成AppResponse，作为调用结果而存在
                AppResponse result = new AppResponse(invocation);

                // 如果调用发生异常，则将异常记录下来
                if (t != null) {
                    if (t instanceof CompletionException) {
                        result.setException(t.getCause());
                    } else {
                        result.setException(t);
                    }
                }

                // 如果没有发生异常，则直接将结果设置到result中
                else {
                    result.setValue(obj);
                }

                // 返回结果
                return result;
            });

            // 将CompletableFuture<AppResponse>包装成异步Rpc结果/AsyncRpcResult并返回
            return new AsyncRpcResult(appResponseFuture, invocation);
        } catch (InvocationTargetException e) {
            if (RpcContext.getContext().isAsyncStarted() && !RpcContext.getContext().stopAsync()) {
                logger.error("Provider async started, but got an exception from the original method, cannot write the exception back to consumer because an async result may have returned the new thread.", e);
            }
            return AsyncRpcResult.newDefaultAsyncResult(null, e.getTargetException(), invocation);
        } catch (Throwable e) {
            throw new RpcException("Failed to invoke remote proxy method " + invocation.getMethodName() + " to " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

	private CompletableFuture<Object> wrapWithFuture(Object value) {

        // 如果是异步开始，则直接获取内部Future返回
        if (RpcContext.getContext().isAsyncStarted()) {
            return ((AsyncContextImpl)(RpcContext.getContext().getAsyncContext())).getInternalFuture();
        }

        // 如果value本身就是CompletableFuture的实现，则强转后直接返回
        else if (value instanceof CompletableFuture) {
            return (CompletableFuture<Object>)value;
        }

        // 否则使用CompletableFuture包装value并返回
        return CompletableFuture.completedFuture(value);
    }

    protected abstract Object doInvoke(T proxy, String methodName, Class<?>[] parameterTypes, Object[] arguments) throws Throwable;

    @Override
    public String toString() {
        return getInterface() + " -> " + (getUrl() == null ? " " : getUrl().toString());
    }


}
