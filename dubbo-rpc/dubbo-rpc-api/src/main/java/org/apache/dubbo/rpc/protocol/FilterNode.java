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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ListenableFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

/**
 * 过滤器节点，实际的{@link #invoker}将作为过滤器链路的最后一个节点，也就是过滤器链路的最后一个节点中，{@link #invoker}等于{@link #next}
 * <p>
 * filter1 -> filter2 -> ... -> filterN -> invoker
 *
 * @see org.apache.dubbo.rpc.protocol.ProtocolFilterWrapper
 */
class FilterNode<T> implements Invoker<T>{
    private final Invoker<T> invoker;
    private final Invoker<T> next;
    private final Filter filter;
    
    public FilterNode(final Invoker<T> invoker, final Invoker<T> next, final Filter filter) {
        this.invoker = invoker;
        this.next = next;
        this.filter = filter;
    }

    @Override
    public Class<T> getInterface() {
        return invoker.getInterface();
    }

    @Override
    public URL getUrl() {
        return invoker.getUrl();
    }

    @Override
    public boolean isAvailable() {
        return invoker.isAvailable();
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        Result asyncResult;
        try {

            // 通过过滤器的invoke方法进行调用，并返回调用结果
            // 每个FilterNode都会包含有一个filter（Filter类型）和一个next（FilterNode类型）
            // 每次调用filter.invoke时，也会把next传入，在过滤过滤执行完成后，就会调用next.invoke，即开始下一个Filter节点的过滤逻辑
            // 直到最后一个节点，最后一个节点的filter是ListenerInvokerWrapper，这个filter一般也是第一个节点的invoker
            // 因此，如果不关注过滤器，可以直接跳到ListenerInvokerWrapper.invoke
            asyncResult = filter.invoke(next, invocation);
        } catch (Exception e) {

            // 过滤器消费异常信息
            if (filter instanceof ListenableFilter) {
                ListenableFilter listenableFilter = ((ListenableFilter) filter);
                try {
                    Filter.Listener listener = listenableFilter.listener(invocation);
                    if (listener != null) {
                        listener.onError(e, invoker, invocation);
                    }
                } finally {
                    listenableFilter.removeListener(invocation);
                }
            } else if (filter instanceof Filter.Listener) {
                Filter.Listener listener = (Filter.Listener) filter;
                listener.onError(e, invoker, invocation);
            }

            // 抛出异常
            throw e;
        } finally {

        }

        // 获取响应任务完成后的任务，其实是通过CompletableFuture.whenComplete添加完成的后置处理
        return asyncResult.whenCompleteWithContext((r, t) -> {

            // 过滤器消费响应结果/异常
            if (filter instanceof ListenableFilter) {
                ListenableFilter listenableFilter = ((ListenableFilter) filter);
                Filter.Listener listener = listenableFilter.listener(invocation);
                try {
                    if (listener != null) {
                        if (t == null) {
                            listener.onResponse(r, invoker, invocation);
                        } else {
                            listener.onError(t, invoker, invocation);
                        }
                    }
                } finally {
                    listenableFilter.removeListener(invocation);
                }
            }

            // 消费结果/异常
            else if (filter instanceof Filter.Listener) {
                Filter.Listener listener = (Filter.Listener) filter;
                if (t == null) {
                    listener.onResponse(r, invoker, invocation);
                } else {
                    listener.onError(t, invoker, invocation);
                }
            }
        });
    }

    @Override
    public void destroy() {
        invoker.destroy();
    }

    @Override
    public String toString() {
        return invoker.toString();
    }

}
