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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_LOADBALANCE;
import static org.apache.dubbo.common.constants.CommonConstants.LOADBALANCE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_AVAILABLE_CHECK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_STICKY_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_STICKY;

/**
 * 集群的抽象实现
 * <p>
 * AbstractClusterInvoker
 */
public abstract class AbstractClusterInvoker<T> implements ClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractClusterInvoker.class);

    protected Directory<T> directory;

    protected boolean availablecheck;

    private AtomicBoolean destroyed = new AtomicBoolean(false);

    private volatile Invoker<T> stickyInvoker = null;

    public AbstractClusterInvoker() {
    }

    public AbstractClusterInvoker(Directory<T> directory) {
        this(directory, directory.getUrl());
    }

    public AbstractClusterInvoker(Directory<T> directory, URL url) {
        if (directory == null) {
            throw new IllegalArgumentException("service directory == null");
        }

        this.directory = directory;
        //sticky: invoker.isAvailable() should always be checked before using when availablecheck is true.
        this.availablecheck = url.getParameter(CLUSTER_AVAILABLE_CHECK_KEY, DEFAULT_CLUSTER_AVAILABLE_CHECK);
    }

    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public URL getUrl() {
        return directory.getConsumerUrl();
    }

    public URL getRegistryUrl() {
        return directory.getUrl();
    }

    @Override
    public boolean isAvailable() {
        Invoker<T> invoker = stickyInvoker;
        if (invoker != null) {
            return invoker.isAvailable();
        }
        return directory.isAvailable();
    }

    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            directory.destroy();
        }
    }

    @Override
    public boolean isDestroyed() {
        return destroyed.get();
    }

    /**
     * Select a invoker using loadbalance policy.</br>
     * a) Firstly, select an invoker using loadbalance. If this invoker is in previously selected list, or,
     * if this invoker is unavailable, then continue step b (reselect), otherwise return the first selected invoker</br>
     * <p>
     * b) Reselection, the validation rule for reselection: selected > available. This rule guarantees that
     * the selected invoker has the minimum chance to be one in the previously selected list, and also
     * guarantees this invoker is available.
     *
     * <ol>
     *     提供给子类调用，进行{@code invokers}筛选，其中包含粘滞连接和负载均衡逻辑
     *     <li>优先粘滞连接</li>
     *     <li>然后在{@link #doSelect}中进行负载均衡</li>
     *     <li>如果负载均衡出的{@code invoker}不可用，则通过{@link #reselect}再次进行选择，此时以{@code invoker}的可用性优先</li>
     * </ol>
     *
     * @param loadbalance load balance policy
     * @param invocation  invocation
     * @param invokers    invoker candidates
     * @param selected    exclude selected invokers or not
     * @return the invoker which will final to do invoke.
     * @throws RpcException exception
     */
    protected Invoker<T> select(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {

        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }

        // 获取方法名称
        String methodName = invocation == null ? StringUtils.EMPTY_STRING : invocation.getMethodName();

        // 是否粘滞连接，也就是consumer会尽可能调用同一个invoker/provider，除非对应的invoker/provider不可用
        // todo 为什么是从invokers中获取这个配置，而不是从consumer的url上获取配置？是组装invokers的时候consumerUrl上的某些配置会添加到invokers
        //  上吗？如果是从invokers上获取配置，不同invoker的配置可能不一样，这样不会不稳定吗？
        boolean sticky =
            invokers.get(0).getUrl().getMethodParameter(methodName, CLUSTER_STICKY_KEY, DEFAULT_CLUSTER_STICKY);

        // 如果之前粘滞的invoker，不在当前的invokers集合中，说明对应的invoker可能不可用/被ban掉，置空保存的粘滞invoker
        // ignore overloaded method
        if (stickyInvoker != null && !invokers.contains(stickyInvoker)) {
            stickyInvoker = null;
        }

        // 需要粘滞连接 && 粘滞invoker可用 && 粘滞invoker不包含在已使用invoker的集合中
        // ignore concurrency problem
        if (sticky && stickyInvoker != null && (selected == null || !selected.contains(stickyInvoker))) {

            // 需要进行可用性检查 && 粘滞invoker可用
            // 条件符合则直接返回粘滞invoker
            if (availablecheck && stickyInvoker.isAvailable()) {
                return stickyInvoker;
            }
        }

        // 通过负载均衡和二次选择，获取到一个可用的invoker
        Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);

        // 如果需要进行粘滞连接，将选择出的invoker作为粘滞invoker
        if (sticky) {
            stickyInvoker = invoker;
        }
        return invoker;
    }

    /**
     * <ol>
     *     <li>通过{@link LoadBalance#select 负载均衡}获取到一个invoker</li>
     *     <li>如果invoker不能被使用，则通过{@link #reselect 重选择}重新获取一个invoker</li>
     *     <li>如果{@link #reselect 重选择}没有获取到一个invoker，那么直接通过索引获取通过{@link LoadBalance#select 负载均衡}获取到的invoker
     *     ，在集合中后一个invoker（不进行任何校验）</li>
     *     <li>返回前三步获取到的invoker</li>
     * </ol>
     *
     * @see LoadBalance#select(List, URL, Invocation)
     * @see #reselect(LoadBalance, Invocation, List, List, boolean)
     */
    private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers,
        List<Invoker<T>> selected) throws RpcException {

        // invokers集合为空，直接返回
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }

        // 只有一个，则直接返回第一个invoker
        if (invokers.size() == 1) {
            return invokers.get(0);
        }

        // 通过负载均衡选择一个invoker
        Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);

        // 判断invoker是否可以被使用：invoker在已选择/使用的invoker集合中 || 需要进行可用性校验且invoker不可用
        //If the `invoker` is in the  `selected` or invoker is unavailable && availablecheck is true, reselect.
        if ((selected != null && selected.contains(invoker)) || (!invoker.isAvailable() && getUrl() != null
            && availablecheck)) {
            try {

                // 通过reselect重新获取一个rInvoker
                Invoker<T> rInvoker = reselect(loadbalance, invocation, invokers, selected, availablecheck);

                // 如果重新获取到的rInvoker不为空，则赋值给invoker
                if (rInvoker != null) {
                    invoker = rInvoker;
                }

                // 如果重新获取到的rInvoker为空，说明无法获取到一个可以使用的invoker，需要进行兜底，通过索引获取到负载均衡invoker后一个的invoker
                else {
                    //Check the index of current selected invoker, if it's not the last one, choose the one at index+1.
                    int index = invokers.indexOf(invoker);
                    try {

                        // 避免索引越界：如果负载均衡选择到的invoker是invokers集合中的最后一个invoker，那么直接index + 1获取下一个invoker会导致索引越界
                        //Avoid collision
                        invoker = invokers.get((index + 1) % invokers.size());
                    } catch (Exception e) {
                        logger.warn(e.getMessage() + " may because invokers list dynamic change, ignore.", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("cluster reselect fail reason is :" + t.getMessage()
                    + " if can not solve, you can set cluster.availablecheck=false in url", t);
            }
        }

        // 返回选择到的invoker
        return invoker;
    }

    /**
     * Reselect, use invokers not in `selected` first, if all invokers are in `selected`,
     * just pick an available one using loadbalance policy.
     *
     * <ol>
     *     <li>从{@code invokers}集合中筛选出未被使用{@code availablecheck 且可用}的invoker，如果有则进行负载均衡并返回</li>
     *     <li>从{@code selected 已选择/使用的invoker集合}中获取可用的invoker，如果有则进行负载均衡并返回</li>
     *     <li>如果都没有，直接返回{@code null}</li>
     * </ol>
     *
     * @param loadbalance    load balance policy
     * @param invocation     invocation
     * @param invokers       invoker candidates
     * @param selected       exclude selected invokers or not
     * @param availablecheck check invoker available if true
     * @return the reselect result to do invoke
     * @throws RpcException exception
     */
    private Invoker<T> reselect(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected, boolean availablecheck) throws RpcException {

        //Allocating one in advance, this list is certain to be used.
        List<Invoker<T>> reselectInvokers = new ArrayList<>(
                invokers.size() > 1 ? (invokers.size() - 1) : invokers.size());

        // 从invokers集合中筛选接下来可以被使用的invoker
        // First, try picking a invoker not in `selected`.
        for (Invoker<T> invoker : invokers) {

            // 可用性检查
            if (availablecheck && !invoker.isAvailable()) {
                continue;
            }

            // 没有在已选择/使用的集合中
            if (selected == null || !selected.contains(invoker)) {
                reselectInvokers.add(invoker);
            }
        }

        // 如果重选择的invoker集合不为空，则通过负载均衡器选择出一个接下来使用的invoker
        if (!reselectInvokers.isEmpty()) {
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 到此处说明重选择的invoker集合为空，可能是所有的invoker都被使用过了，那么考虑从已选择/使用的集合中获取可用的invoker

        // 从selected集合中获取可用的invoker
        // Just pick an available invoker using loadbalance policy
        if (selected != null) {
            for (Invoker<T> invoker : selected) {

                // 可用性检查，并进行去重
                // available first
                if ((invoker.isAvailable()) && !reselectInvokers.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        }

        // 如果从已选择/使用的集合中有获取到可用的invoker，则再次通过负载均衡获取可用的invoker
        if (!reselectInvokers.isEmpty()) {
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 否则返回空
        return null;
    }

    @Override
    public Result invoke(final Invocation invocation) throws RpcException {

        // 校验状态
        checkWhetherDestroyed();

        // 获取当前线程上下文的附件，绑定到invocation上
        // binding attachments into invocation.
        Map<String, Object> contextAttachments = RpcContext.getContext().getObjectAttachments();
        if (contextAttachments != null && contextAttachments.size() != 0) {
            ((RpcInvocation) invocation).addObjectAttachments(contextAttachments);
        }

        // 获取当前可调用的服务提供者invoker集合
        List<Invoker<T>> invokers = list(invocation);

        // 根据服务提供者集合invokers获取负载均衡器，默认值是random负载均衡器
        LoadBalance loadbalance = initLoadBalance(invokers, invocation);

        // 幂等处理，主要是用来生成请求id，标识此次请求，id也会传输给提供者，然后提供者会在响应中带上该id，让我们能知道响应对应的是哪个请求的
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);

        // 进行实际的调用操作：默认走的是FailoverClusterInvoker.doInvoke
        return doInvoke(invocation, invokers, loadbalance);
    }

    protected void checkWhetherDestroyed() {
        if (destroyed.get()) {
            throw new RpcException("Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
                    + " use dubbo version " + Version.getVersion()
                    + " is now destroyed! Can not invoke any more.");
        }
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + getUrl().toString();
    }

    protected void checkInvokers(List<Invoker<T>> invokers, Invocation invocation) {
        if (CollectionUtils.isEmpty(invokers)) {
            throw new RpcException(RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER,
                "Failed to invoke the method " + invocation.getMethodName() + " in the service "
                    + getInterface().getName() + ". No provider available for the service " + directory
                    .getConsumerUrl()
                    .getServiceKey() + " from registry " + directory.getUrl().getAddress() + " on the consumer "
                    + NetUtils.getLocalHost() + " using the dubbo version " + Version.getVersion()
                    + ". Please check if the providers have been started and registered.");
        }
    }

    protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance)
        throws RpcException;

    /**
     * 通过{@link #directory}获取可用的{@code invokers}，内部会通过{@link org.apache.dubbo.rpc.cluster.Router}进行路由过滤
     */
    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        return directory.list(invocation);
    }

    /**
     * Init LoadBalance.
     * <p>
     * if invokers is not empty, init from the first invoke's url and invocation
     * if invokes is empty, init a default LoadBalance(RandomLoadBalance)
     * </p>
     *
     * @param invokers   invokers
     * @param invocation invocation
     * @return LoadBalance instance. if not need init, return null.
     */
    protected LoadBalance initLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {

        // 如果invokers不为空
        if (CollectionUtils.isNotEmpty(invokers)) {

            // 从第一个invoker的url上，对应的方法后获取loadbalance对应的key，默认值是random
            return ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(invokers.get(0).getUrl()
                    .getMethodParameter(RpcUtils.getMethodName(invocation), LOADBALANCE_KEY, DEFAULT_LOADBALANCE));
        } else {

            // 如果invoker为空，则直接加载默认的random负载均衡器
            return ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(DEFAULT_LOADBALANCE);
        }
    }
}
