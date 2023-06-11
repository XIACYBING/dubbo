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
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.TimeoutCountDown;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_ATTACHMENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.FORCE_USE_TAG;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;


/**
 * provider端的{@link RpcContext}加载的过滤器，会加载consumer和provider两端的上下文，并在调用结束后清除consumer和provider两端的上下文信息
 * <p>
 * ContextFilter set the provider RpcContext with invoker, invocation, local port it is using and host for
 * current execution thread.
 *
 * @see RpcContext
 */
@Activate(group = PROVIDER, order = Integer.MIN_VALUE)
public class ContextFilter implements Filter, Filter.Listener {

    private static final String TAG_KEY = "dubbo.tag";

    private static final Set<String> UNLOADING_KEYS;

    static {
        UNLOADING_KEYS = new HashSet<>(128);
        UNLOADING_KEYS.add(PATH_KEY);
        UNLOADING_KEYS.add(INTERFACE_KEY);
        UNLOADING_KEYS.add(GROUP_KEY);
        UNLOADING_KEYS.add(VERSION_KEY);
        UNLOADING_KEYS.add(DUBBO_VERSION_KEY);
        UNLOADING_KEYS.add(TOKEN_KEY);
        UNLOADING_KEYS.add(TIMEOUT_KEY);
        UNLOADING_KEYS.add(TIMEOUT_ATTACHMENT_KEY);

        // Remove async property to avoid being passed to the following invoke chain.
        UNLOADING_KEYS.add(ASYNC_KEY);
        UNLOADING_KEYS.add(TAG_KEY);
        UNLOADING_KEYS.add(FORCE_USE_TAG);
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        // 获取调用请求中携带的附件信息
        Map<String, Object> attachments = invocation.getObjectAttachments();

        // 如果附件信息不为空，则需要过滤掉UNLOADING_KEYS中设置的不加载的key
        if (attachments != null) {
            Map<String, Object> newAttach = new HashMap<>(attachments.size());
            for (Map.Entry<String, Object> entry : attachments.entrySet()) {
                String key = entry.getKey();
                if (!UNLOADING_KEYS.contains(key)) {
                    newAttach.put(key, entry.getValue());
                }
            }
            attachments = newAttach;
        }

        // 设置调用方的上下文信息
        RpcContext context = RpcContext.getContext();
        context.setInvoker(invoker).setInvocation(invocation)
               //                .setAttachments(attachments)  // merged from dubbox
               .setLocalAddress(invoker.getUrl().getHost(), invoker.getUrl().getPort());

        // 设置远程应用key：当前是在provider，这里的远程应用key是从consumer的角度出发的
        String remoteApplication = (String)invocation.getAttachment(REMOTE_APPLICATION_KEY);
        if (StringUtils.isNotEmpty(remoteApplication)) {
            context.setRemoteApplicationName(remoteApplication);
        } else {
            context.setRemoteApplicationName((String)context.getAttachment(REMOTE_APPLICATION_KEY));
        }

        // 当前为Provider端，如果接收到的请求中有配置超时限制，则向consumer上下文中增加一个超时倒计时器，并在TimeoutFilter中判断超时情况
        long timeout = RpcUtils.getTimeout(invocation, -1);
        if (timeout != -1) {
            context.set(TIME_COUNTDOWN_KEY, TimeoutCountDown.newCountDown(timeout, TimeUnit.MILLISECONDS));
        }

        // dubbox的某些链路中，会在前置缓解中设置附件信息，所以需要合并
        // merged from dubbox
        // we may already added some attachments into RpcContext before this filter (e.g. in rest protocol)
        if (attachments != null) {
            if (context.getObjectAttachments() != null) {
                context.getObjectAttachments().putAll(attachments);
            } else {
                context.setObjectAttachments(attachments);
            }
        }

        // 设置invoker信息，当前invoker代表provider最终要调用的invoker
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(invoker);
        }

        try {

            // 设置consumer的上下文信息中的canRemove为false，避免调用过程中consumer上下文信息被清除
            context.clearAfterEachInvoke(false);
            return invoker.invoke(invocation);
        } finally {

            // 设置consumer上下文信息中的canRemove为true，因为当前调用已经完成，上下文信息可以被清除
            context.clearAfterEachInvoke(true);

            // 清除consumer和provider的上下文信息
            // IMPORTANT! For async scenario, we must remove context from current thread, so we always create a new RpcContext for the next invoke for the same thread.
            RpcContext.removeContext(true);
            RpcContext.removeServerContext();
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

        // 将provider端的上下文信息放入响应中传递给consumer
        // pass attachments to result
        appResponse.addObjectAttachments(RpcContext.getServerContext().getObjectAttachments());
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
