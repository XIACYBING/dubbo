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

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;

/**
 * consumer调用provider过程中，设置相关属性的拦截器
 */
@Activate
public class ConsumerContextClusterInterceptor implements ClusterInterceptor, ClusterInterceptor.Listener {

    public static final String NAME = "context";

    @Override
    public void before(AbstractClusterInvoker<?> invoker, Invocation invocation) {

        // 设置invoker
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation)invocation).setInvoker(invoker);
        }

        // 移除之前请求记录的provider的server上下文
        RpcContext.removeServerContext();
    }

    @Override
    public void after(AbstractClusterInvoker<?> clusterInvoker, Invocation invocation) {

        // 移除consumer的上下文信息
        RpcContext.removeContext(true);
    }

    @Override
    public void onMessage(Result appResponse, AbstractClusterInvoker<?> invoker, Invocation invocation) {

        // 正常响应，将响应信息中的对象附件添加到当前线程的provider的server上下文
        RpcContext.getServerContext().setObjectAttachments(appResponse.getObjectAttachments());
    }

    @Override
    public void onError(Throwable t, AbstractClusterInvoker<?> invoker, Invocation invocation) {

    }
}
