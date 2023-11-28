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
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

/**
 * BroadcastClusterInvoker
 * <p>
 * 广播：适用于某些通知场景，当前会调用所有invoker并发起请求，如果有异常，则会根据配置情况在特定时机抛出
 */
public class BroadcastClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(BroadcastClusterInvoker.class);
    private static final String BROADCAST_FAIL_PERCENT_KEY = "broadcast.fail.percent";
    private static final int MAX_BROADCAST_FAIL_PERCENT = 100;
    private static final int MIN_BROADCAST_FAIL_PERCENT = 0;

    public BroadcastClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {

        // 校验invokers状态
        checkInvokers(invokers, invocation);

        // 设置当前invokers调用
        RpcContext.getContext().setInvokers((List)invokers);

        // 记录异常信息，如果有多个invoker调用发生异常，则后者会覆盖前者
        RpcException exception = null;

        // 调用结果
        Result result = null;
        URL url = getUrl();

        // 广播失败百分比：范围是0-100，如果是100，代表异常只会在所有invoker调用完成后抛出，如果是0则代表异常会在出现时就对外抛出
        // 如果是0-100之间，则会根据invokers的数量计算出一个门槛，当出现异常时，且调用invoker的数量达到门槛，则将异常抛出
        // The value range of broadcast.fail.threshold must be 0～100.
        // 100 means that an exception will be thrown last, and 0 means that as long as an exception occurs, it will be thrown.
        // see https://github.com/apache/dubbo/pull/7174
        int broadcastFailPercent = url.getParameter(BROADCAST_FAIL_PERCENT_KEY, MAX_BROADCAST_FAIL_PERCENT);

        // 校验百分比
        if (broadcastFailPercent < MIN_BROADCAST_FAIL_PERCENT || broadcastFailPercent > MAX_BROADCAST_FAIL_PERCENT) {
            logger.info(String.format(
                "The value corresponding to the broadcast.fail.percent parameter must be between 0 and 100. "
                    + "The current setting is %s, which is reset to 100.", broadcastFailPercent));
            broadcastFailPercent = MAX_BROADCAST_FAIL_PERCENT;
        }

        // 计算异常抛出的门槛
        int failThresholdIndex = invokers.size() * broadcastFailPercent / MAX_BROADCAST_FAIL_PERCENT;

        // 调用异常的invoker数量
        int failIndex = 0;

        // 循环invokers，发起请求
        for (Invoker<T> invoker : invokers) {
            try {

                // 发起请求
                result = invoker.invoke(invocation);

                // 如果请求有异常
                if (null != result && result.hasException()) {
                    Throwable resultException = result.getException();
                    if (null != resultException) {

                        // 获取或包装成RpcException
                        exception = getRpcException(result.getException());
                        logger.warn(exception.getMessage(), exception);

                        // 失败的invoker数量已经达到门槛，中断循环
                        if (failIndex == failThresholdIndex) {
                            break;
                        }

                        // 自增失败的invoker数量，继续循环
                        else {
                            failIndex++;
                        }
                    }
                }
            } catch (Throwable e) {

                // 获取或包装成RpcException
                exception = getRpcException(e);
                logger.warn(exception.getMessage(), exception);

                // 失败的invoker数量已经达到门槛，中断循环
                if (failIndex == failThresholdIndex) {
                    break;
                }

                // 自增失败的invoker数量，继续循环
                else {
                    failIndex++;
                }
            }
        }

        // 异常不等于空，抛出异常
        if (exception != null) {
            if (failIndex == failThresholdIndex) {
                logger.debug(
                        String.format("The number of BroadcastCluster call failures has reached the threshold %s", failThresholdIndex));
            } else {
                logger.debug(String.format("The number of BroadcastCluster call failures has not reached the threshold %s, fail size is %s",
                        failThresholdIndex, failIndex));
            }
            throw exception;
        }

        // 否则返回结果
        return result;
    }

    private RpcException getRpcException(Throwable throwable) {
        RpcException rpcException = null;
        if (throwable instanceof RpcException) {
            rpcException = (RpcException) throwable;
        } else {
            rpcException = new RpcException(throwable.getMessage(), throwable);
        }
        return rpcException;
    }
}
