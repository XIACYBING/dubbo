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
package org.apache.dubbo.rpc.cluster.interceptor;

import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.cluster.support.wrapper.AbstractCluster;

/**
 * Different from {@link Filter}, ClusterInterceptor works at the outmost layer, before one specific address/invoker is picked.
 *
 * @see ConsumerContextClusterInterceptor
 * @see ZoneAwareClusterInterceptor
 */
@SPI
@SuppressWarnings(value = {"AlibabaAbstractMethodOrInterfaceMethodMustUseJavadoc", "AlibabaClassMustHaveAuthor"})
public interface ClusterInterceptor {

    /**
     * 前置拦截
     */
    void before(AbstractClusterInvoker<?> clusterInvoker, Invocation invocation);

    /**
     * 后置拦截
     */
    void after(AbstractClusterInvoker<?> clusterInvoker, Invocation invocation);

    /**
     * Does not need to override this method, override {@link #before(AbstractClusterInvoker, Invocation)}
     * and {@link #after(AbstractClusterInvoker, Invocation)}, methods to add your own logic expected to be
     * executed before and after invoke.
     * <p>
     * 进行实际的调用
     */
    default Result intercept(AbstractClusterInvoker<?> clusterInvoker, Invocation invocation) throws RpcException {
        return clusterInvoker.invoke(invocation);
    }

    /**
     * 用来监听请求正常和异常的结果，当前监听器一般和{@link ClusterInterceptor}一起被实现，这样才能正常的被shiyong
     *
     * @see AbstractCluster.InterceptorInvokerNode#invoke(org.apache.dubbo.rpc.Invocation)
     */
    interface Listener {

        /**
         * 监听正常请求
         */
        void onMessage(Result appResponse, AbstractClusterInvoker<?> clusterInvoker, Invocation invocation);

        /**
         * 监听异常请求
         */
        void onError(Throwable t, AbstractClusterInvoker<?> clusterInvoker, Invocation invocation);
    }
}
