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
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;

import static org.apache.dubbo.rpc.Constants.EXECUTES_KEY;


/**
 * 和{@link ActiveLimitFilter}相对，{@link ExecuteLimitFilter}是Dubbo在provider端对限流的实现
 * <p>
 * The maximum parallel execution request count per method per service for the provider.If the max configured
 * <b>executes</b> is set to 10 and if invoke request where it is already 10 then it will throws exception. It
 * continue the same behaviour un till it is <10.
 */
@Activate(group = CommonConstants.PROVIDER, value = EXECUTES_KEY)
public class ExecuteLimitFilter implements Filter, Filter.Listener {

    private static final String EXECUTE_LIMIT_FILTER_START_TIME = "execute_limit_filter_start_time";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        // 获取当前调用URL
        URL url = invoker.getUrl();

        // 获取方法名称
        String methodName = invocation.getMethodName();

        // 获取方法的并发上限，默认值是0，即不限制
        int max = url.getMethodParameter(methodName, EXECUTES_KEY, 0);

        // 尝试增加并发度，如果失败，直接抛出异常
        if (!RpcStatus.beginCount(url, methodName, max)) {
            throw new RpcException(RpcException.LIMIT_EXCEEDED_EXCEPTION,
                "Failed to invoke method " + invocation.getMethodName() + " in provider " + url
                    + ", cause: The service using threads greater than <dubbo:service executes=\"" + max
                    + "\" /> limited.");
        }

        // 记录开始调用时间，会在onResponse和onError方法中减少并发度，并计算调用耗时
        invocation.put(EXECUTE_LIMIT_FILTER_START_TIME, System.currentTimeMillis());
        try {
            return invoker.invoke(invocation);
        } catch (Throwable t) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RpcException("unexpected exception when ExecuteLimitFilter", t);
            }
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

        // 结束调用，减少并发度
        RpcStatus.endCount(invoker.getUrl(), invocation.getMethodName(), getElapsed(invocation), true);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (t instanceof RpcException) {
            RpcException rpcException = (RpcException)t;

            // 如果limitExceed为true，说明是并发达到上限抛出的异常，无需处理并发度和调用耗时
            if (rpcException.isLimitExceed()) {
                return;
            }
        }

        // 结束调用，减少并发度
        RpcStatus.endCount(invoker.getUrl(), invocation.getMethodName(), getElapsed(invocation), false);
    }

    /**
     * 计算调用耗时
     */
    private long getElapsed(Invocation invocation) {
        Object beginTime = invocation.get(EXECUTE_LIMIT_FILTER_START_TIME);
        return beginTime != null ? System.currentTimeMillis() - (Long)beginTime : 0;
    }
}
