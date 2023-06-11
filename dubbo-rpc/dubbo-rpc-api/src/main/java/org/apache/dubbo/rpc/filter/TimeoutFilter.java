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

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.TimeoutCountDown;

import java.util.Arrays;

import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;

/**
 * provider端的一个超时过滤器
 * <p>
 * Log any invocation timeout, but don't stop server from running
 */
@Activate(group = CommonConstants.PROVIDER)
public class TimeoutFilter implements Filter, Filter.Listener {

    private static final Logger logger = LoggerFactory.getLogger(TimeoutFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

        // 请求已经结束，需要判断当前是否超时
        Object obj = RpcContext.getContext().get(TIME_COUNTDOWN_KEY);
        if (obj != null) {
            TimeoutCountDown countDown = (TimeoutCountDown)obj;

            // 如果当前已经超时，清除响应数据，并打印日志    todo consumer端对这种情况怎么处理？这里好像并没有再进行超时标识的设置？会和请求的超时检查任务联动？还是有其他处理方案？
            if (countDown.isExpired()) {

                // 清除响应数据、异常信息和附件信息
                // clear response in case of timeout.
                ((AppResponse)appResponse).clear();
                if (logger.isWarnEnabled()) {
                    logger.warn(
                        "invoke timed out. method: " + invocation.getMethodName() + " arguments: " + Arrays.toString(
                            invocation.getArguments()) + " , url is " + invoker.getUrl() + ", invoke elapsed "
                            + countDown.elapsedMillis() + " ms.");
                }
            }
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
