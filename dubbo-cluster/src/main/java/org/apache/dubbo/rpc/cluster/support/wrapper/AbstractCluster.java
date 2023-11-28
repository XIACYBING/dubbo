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
package org.apache.dubbo.rpc.cluster.support.wrapper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.interceptor.ClusterInterceptor;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.REFERENCE_INTERCEPTOR_KEY;

/**
 * @see AbstractClusterInvoker
 */
public abstract class AbstractCluster implements Cluster {

    /**
     * 将最终执行invoker的{@code clusterInvoker}和集群拦截器包装在一起，称为一条拦截链，然后返回
     */
    private <T> Invoker<T> buildClusterInterceptors(AbstractClusterInvoker<T> clusterInvoker, String key) {
        AbstractClusterInvoker<T> last = clusterInvoker;

        // 根据消费者url上的参数配置获取集群拦截器
        List<ClusterInterceptor> interceptors = ExtensionLoader
            .getExtensionLoader(ClusterInterceptor.class)
            .getActivateExtension(clusterInvoker.getUrl(), key);

        // 倒序循环集群拦截器，构建node，参数clusterInvoker将在最后一个被调用，或者说最后一个InterceptorInvokerNode中的next属性就是最终执行请求的invoker
        if (!interceptors.isEmpty()) {
            for (int i = interceptors.size() - 1; i >= 0; i--) {
                final ClusterInterceptor interceptor = interceptors.get(i);
                final AbstractClusterInvoker<T> next = last;
                last = new InterceptorInvokerNode<>(clusterInvoker, interceptor, next);
            }
        }
        return last;
    }

    @Override
    public <T> Invoker<T> join(Directory<T> directory) throws RpcException {
        return buildClusterInterceptors(doJoin(directory), directory.getUrl().getParameter(REFERENCE_INTERCEPTOR_KEY));
    }

    /**
     * 交由子类实现，从{@code directory}中获取到实际的{@code invokers}，然后通过{@link org.apache.dubbo.rpc.cluster.Router}进行路由，通过
     * {@link LoadBalance}进行负载均衡，最终得到需要执行请求的{@code invoker}
     */
    protected abstract <T> AbstractClusterInvoker<T> doJoin(Directory<T> directory) throws RpcException;

    protected class InterceptorInvokerNode<T> extends AbstractClusterInvoker<T> {

        /**
         * 最终执行请求的invoker，在拦截器链路中每个节点都会有，方便输出当前链路所代表的invoker的相关信息
         */
        private AbstractClusterInvoker<T> clusterInvoker;

        /**
         * 当前的拦截器
         */
        private ClusterInterceptor interceptor;

        /**
         * 下一个节点，拦截链路中，倒数第二个的next就是执行请求的invoker
         */
        private AbstractClusterInvoker<T> next;

        public InterceptorInvokerNode(AbstractClusterInvoker<T> clusterInvoker, ClusterInterceptor interceptor,
            AbstractClusterInvoker<T> next) {
            this.clusterInvoker = clusterInvoker;
            this.interceptor = interceptor;
            this.next = next;
        }

        @Override
        public Class<T> getInterface() {
            return clusterInvoker.getInterface();
        }

        @Override
        public URL getUrl() {
            return clusterInvoker.getUrl();
        }

        @Override
        public boolean isAvailable() {
            return clusterInvoker.isAvailable();
        }

        @Override
        public Result invoke(Invocation invocation) throws RpcException {
            Result asyncResult;
            try {

                // 拦截的前置操作：将next设置到RpcInvocation（如果invocation是RpcInvocation）中并清理serverContext
                interceptor.before(next, invocation);

                //调用next的invoke，如果有多个拦截器，当前链路会像AOP一样进行interceptor.before -> interceptor.intercept -> next.invoke ->
                // nextInterceptor.before -> nextInterceptor.intercept -> nextNext.invoke -> ... -> AbstractClusterInvoker.invoke
                // 即：最后会进入到AbstractClusterInvoker.invoke方法
                asyncResult = interceptor.intercept(next, invocation);
            } catch (Exception e) {

                // 发起请求流程中的异常回调
                // onError callback
                if (interceptor instanceof ClusterInterceptor.Listener) {
                    ClusterInterceptor.Listener listener = (ClusterInterceptor.Listener)interceptor;
                    listener.onError(e, clusterInvoker, invocation);
                }
                throw e;
            } finally {

                // 拦截的后置处理：清理资源
                interceptor.after(next, invocation);
            }

            // 响应的回调监听，包含正常响应和异常响应
            return asyncResult.whenCompleteWithContext((r, t) -> {
                // onResponse callback
                if (interceptor instanceof ClusterInterceptor.Listener) {
                    ClusterInterceptor.Listener listener = (ClusterInterceptor.Listener) interceptor;
                    if (t == null) {
                        listener.onMessage(r, clusterInvoker, invocation);
                    } else {
                        listener.onError(t, clusterInvoker, invocation);
                    }
                }
            });
        }

        @Override
        public void destroy() {
            clusterInvoker.destroy();
        }

        @Override
        public String toString() {
            return clusterInvoker.toString();
        }

        @Override
        protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
            // The only purpose is to build an interceptor chain, so the cluster related logic doesn't matter.
            return null;
        }
    }
}
