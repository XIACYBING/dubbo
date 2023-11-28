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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.FORKS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FORKS;

/**
 * NOTICE! This implementation does not work well with async call.
 * <p>
 * Invoke a specific number of invokers concurrently, usually used for demanding real-time operations, but need to waste more service resources.
 *
 * <a href="http://en.wikipedia.org/wiki/Fork_(topology)">Fork</a>
 * <p>
 * 按照URL上的{@link CommonConstants#FORKS_KEY}配置，并发发起请求，只要有一个响应，就直接返回
 * <p>
 * 适合于实时性要求高的读，或无并发控制要求的写操作
 */
public class ForkingClusterInvoker<T> extends AbstractClusterInvoker<T> {

    /**
     * Use {@link NamedInternalThreadFactory} to produce {@link org.apache.dubbo.common.threadlocal.InternalThread}
     * which with the use of {@link org.apache.dubbo.common.threadlocal.InternalThreadLocal} in {@link RpcContext}.
     * <p>
     * 并发的线程池
     */
    private final ExecutorService executor = Executors.newCachedThreadPool(
            new NamedInternalThreadFactory("forking-cluster-timer", true));

    public ForkingClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        try {

            // 校验invokers
            checkInvokers(invokers, invocation);

            // 存储最终执行请求的invoker
            final List<Invoker<T>> selected;

            // 并发数，代表发起多少个请求，默认为2
            final int forks = getUrl().getParameter(FORKS_KEY, DEFAULT_FORKS);

            // 超时毫秒数，默认为1000
            final int timeout = getUrl().getParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);

            // 如果并发数小于等于0（不控制并发数），或者当前可用的invoker数量小于并发数，则直接将invokers集合赋予selected，用于后面的请求执行
            if (forks <= 0 || forks >= invokers.size()) {
                selected = invokers;
            }

            // 否则按照并发数，循环进行负载均衡，获得足够数量的invoker
            else {
                selected = new ArrayList<>(forks);
                while (selected.size() < forks) {
                    Invoker<T> invoker = select(loadbalance, invocation, invokers, selected);

                    // 因为多次负载均衡返回的可能时同一个invoker，所以需要去重
                    if (!selected.contains(invoker)) {
                        //Avoid add the same invoker several times.
                        selected.add(invoker);
                    }
                }
            }

            // 设置调用的invoker集合
            RpcContext.getContext().setInvokers((List)selected);

            // 请求异常数量
            final AtomicInteger count = new AtomicInteger();

            // 阻塞队列，响应/异常信息会填充到该队列上
            final BlockingQueue<Object> ref = new LinkedBlockingQueue<>();

            // 循环执行请求的invoker，提交任务到线程池
            for (final Invoker<T> invoker : selected) {
                executor.execute(() -> {
                    try {

                        // 执行请求
                        Result result = invoker.invoke(invocation);

                        // 将结果填充到阻塞队列上
                        ref.offer(result);
                    } catch (Throwable e) {
                        int value = count.incrementAndGet();

                        // 如果失败的请求个数超出了当前发起请求的invoker数量，则向阻塞队列写入异常
                        // 这里主要关注的是等于，当异常请求个数等于发起请求个数，说明所有invoker都请求失败了，则需要向阻塞队列中写入异常，
                        // 而不是等待阻塞队列抛出超时异常
                        if (value >= selected.size()) {
                            ref.offer(e);
                        }
                    }
                });
            }
            try {

                // 阻塞在阻塞队列上，等待获取结果
                Object ret = ref.poll(timeout, TimeUnit.MILLISECONDS);

                // 如果结果是个异常，则包装后对外抛出
                if (ret instanceof Throwable) {
                    Throwable e = (Throwable)ret;
                    throw new RpcException(e instanceof RpcException ? ((RpcException)e).getCode() : 0,
                        "Failed to forking invoke provider " + selected
                            + ", but no luck to perform the invocation. Last error is: " + e.getMessage(),
                        e.getCause() != null ? e.getCause() : e);
                }

                // 否则返回请求的正常结果
                return (Result)ret;
            } catch (InterruptedException e) {
                throw new RpcException("Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e);
            }
        } finally {

            // 清除附件信息
            // clear attachments which is binding to current thread.
            RpcContext.getContext().clearAttachments();
        }
    }
}
