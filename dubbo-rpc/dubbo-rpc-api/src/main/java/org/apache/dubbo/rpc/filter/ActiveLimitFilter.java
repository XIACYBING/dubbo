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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.Constants.ACTIVES_KEY;

/**
 * consumer端负责调用限制的过滤器，在方法纬度上限制并发调用
 *
 * ActiveLimitFilter restrict the concurrent client invocation for a service or service's method from client side.
 * To use active limit filter, configured url with <b>actives</b> and provide valid >0 integer value.
 * <pre>
 *     e.g. <dubbo:reference id="demoService" check="false" interface="org.apache.dubbo.demo.DemoService" "actives"="2"/>
 *      In the above example maximum 2 concurrent invocation is allowed.
 *      If there are more than configured (in this example 2) is trying to invoke remote method, then rest of invocation
 *      will wait for configured timeout(default is 0 second) before invocation gets kill by dubbo.
 * </pre>
 *
 * @see Filter
 */
@Activate(group = CONSUMER, value = ACTIVES_KEY)
public class ActiveLimitFilter implements Filter, Filter.Listener {

    private static final String ACTIVELIMIT_FILTER_START_TIME = "activelimit_filter_start_time";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        // 获取provider的url
        URL url = invoker.getUrl();

        // 获取要调用的方法名称
        String methodName = invocation.getMethodName();

        // 获取url上限制的并发调用数   todo 看provider端的并发限制？这样子不是只在一个consumer应用上限制并发度吗？
        int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);

        // 获取方法对应的RpcStatus
        final RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());

        // 尝试增加并发度，失败的话则需要不断休眠等待并发度降低
        if (!RpcStatus.beginCount(url, methodName, max)) {

            // 获取超时配置
            long timeout = invoker.getUrl().getMethodParameter(invocation.getMethodName(), TIMEOUT_KEY, 0);

            // 记录当前开始时间
            long start = System.currentTimeMillis();

            // 设置超时时间为剩余时间
            long remain = timeout;

            // 对RpcStatus进行同步，如果有多个相同请求阻塞在并发度限制上，只会有一个请求获取到RpcStatus的同步块
            synchronized (rpcStatus) {

                // 再次尝试增加并发度，如果成功，循环结束，如果失败则继续循环
                while (!RpcStatus.beginCount(url, methodName, max)) {

                    // 通过RpcStatus进行休眠，等待其他正在进行中的请求结束，其他请求结束的时候会调用rpcStatus.notifyAll()
                    try {
                        rpcStatus.wait(remain);
                    } catch (InterruptedException e) {
                        // ignore
                    }

                    // 休眠结束，获取休眠的毫秒数
                    long elapsed = System.currentTimeMillis() - start;

                    // 超时毫秒数减去休眠毫秒数，获取剩余可用的毫秒数
                    remain = timeout - elapsed;

                    // 如果当前已经没有可用毫秒数，则直接抛出异常，不进行调用
                    if (remain <= 0) {
                        throw new RpcException(RpcException.LIMIT_EXCEEDED_EXCEPTION,
                            "Waiting concurrent invoke timeout in client-side for service:  " + invoker
                                .getInterface()
                                .getName() + ", method: " + invocation.getMethodName() + ", elapsed: " + elapsed
                                + ", timeout: " + timeout + ". concurrent invokes: " + rpcStatus.getActive()
                                + ". max concurrent invoke limit: " + max);
                    }
                }
            }
        }

        // 记录调用开始毫秒，用于调用结束记录调用耗时
        invocation.put(ACTIVELIMIT_FILTER_START_TIME, System.currentTimeMillis());

        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

        // 调用成功的监听回调

        // 获取方法名称
        String methodName = invocation.getMethodName();
        URL url = invoker.getUrl();

        // 获取最大并发度
        int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);

        // 结束调用，会减少相关方法的并发度记录
        RpcStatus.endCount(url, methodName, getElapsed(invocation), true);

        // 通知其他因并发度不够而阻塞的请求线程
        notifyFinish(RpcStatus.getStatus(url, methodName), max);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

        // 调用错误的监听回调

        String methodName = invocation.getMethodName();
        URL url = invoker.getUrl();
        int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);

        // 如果异常是RpcException，且是因为并发度超时限制导致的异常，那么直接返回，因为是当前过滤器抛出的
        if (t instanceof RpcException) {
            RpcException rpcException = (RpcException)t;
            if (rpcException.isLimitExceed()) {
                return;
            }
        }

        // 结束调用，记录为失败的调用，并计算相关耗时
        RpcStatus.endCount(url, methodName, getElapsed(invocation), false);

        // 通知其他因并发度不够而阻塞的请求线程
        notifyFinish(RpcStatus.getStatus(url, methodName), max);
    }

    /**
     * 获取调用耗时毫秒数
     */
    private long getElapsed(Invocation invocation) {
        Object beginTime = invocation.get(ACTIVELIMIT_FILTER_START_TIME);
        return beginTime != null ? System.currentTimeMillis() - (Long)beginTime : 0;
    }

    /**
     * 通知所有因当前{@code rpcStatus}而阻塞的请求线程
     */
    private void notifyFinish(final RpcStatus rpcStatus, int max) {

        // 如果不限制并发，则无需处理    todo 其实这里不应该增加这个判断，如果请求数量真的达到了Integer.MAX_VALUE，那么确实会有请求因为max而阻塞，并一直阻塞到超时
        if (max > 0) {
            synchronized (rpcStatus) {
                rpcStatus.notifyAll();
            }
        }
    }
}
