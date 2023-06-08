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

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.TimeoutCountDown;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;

/**
 * 应用在consumer端的一个过滤器，主要作用是在{@link RpcContext}中记录调用信息，并在真正的RPC调用发起前检查超时情况
 * <p>
 * todo 因为要检查超时情况，所以当前过滤器应该要贴近实际的RPC调用？
 * <p>
 * ConsumerContextFilter set current RpcContext with invoker,invocation, local host, remote host and port
 * for consumer invoker.It does it to make the requires info available to execution thread's RpcContext.
 *
 * @see org.apache.dubbo.rpc.Filter
 * @see RpcContext
 */
@Activate(group = CONSUMER, order = -10000)
public class ConsumerContextFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        // 获取消费端上下文信息
        RpcContext context = RpcContext.getContext();
        context

            // 记录consumer端的相关信息
            .setInvoker(invoker)
            .setInvocation(invocation)
            .setLocalAddress(NetUtils.getLocalHost(), 0)

            // 记录provider端的相关信息
            .setRemoteAddress(invoker.getUrl().getHost(), invoker.getUrl().getPort())
            .setRemoteApplicationName(invoker.getUrl().getParameter(REMOTE_APPLICATION_KEY))
            .setAttachment(REMOTE_APPLICATION_KEY, invoker.getUrl().getParameter(APPLICATION_KEY));

        // invocation请求关联invoker
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation)invocation).setInvoker(invoker);
        }

        // 检查超时情况，这边如果能获取到countDown，一般是用户本身设置进去的，consumer端并没有设置countDown的链路
        // pass default timeout set by end user (ReferenceConfig)
        Object countDown = context.get(TIME_COUNTDOWN_KEY);
        if (countDown != null) {
            TimeoutCountDown timeoutCountDown = (TimeoutCountDown)countDown;

            // 如果已经超时，则不发起Rpc调用，直接返回一个超时异常的结果
            if (timeoutCountDown.isExpired()) {
                return AsyncRpcResult.newDefaultAsyncResult(new RpcException(RpcException.TIMEOUT_TERMINATE,
                    "No time left for making the following call: " + invocation.getServiceName() + "."
                        + invocation.getMethodName() + ", terminate directly."), invocation);
            }
        }

        // 否则正常发起调用
        return invoker.invoke(invocation);
    }

}
