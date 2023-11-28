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
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RETRIES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;

/**
 * When invoke fails, log the initial error and retry other invokers (retry n times, which means at most n different invokers will be invoked)
 * Note that retry causes latency.
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Failover">Failover</a>
 * <p>
 * 异常时按照配置的重试次数去重新发起请求
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailoverClusterInvoker.class);

    public FailoverClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {

        // 赋值invokers给方法变量
        List<Invoker<T>> copyInvokers = invokers;

        // 校验invoker，如果为空，则直接抛出异常
        checkInvokers(copyInvokers, invocation);

        // 获取要调用的方法名称
        String methodName = RpcUtils.getMethodName(invocation);

        // 计算需要重试的次数，如果没有特殊配置，一般是3
        int len = calculateInvokeTimes(methodName);

        // 开始重试
        // retry loop.

        // last exception，将会记录重试中的最后一个异常
        RpcException le = null;

        // invoked invokers，记录已经调用的提供者invoker
        List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyInvokers.size());

        // 记录已经调用过的provider地址，在重试全部失败的情况下，用于异常内的信息输出
        Set<String> providers = new HashSet<String>(len);
        for (int i = 0; i < len; i++) {
            //Reselect before retry to avoid a change of candidate `invokers`.
            //NOTE: if `invokers` changed, then `invoked` also lose accuracy.

            // 非第一次循环的情况下，需要重新获取可用的invoker，避免某些invoker已经失效，然后选择到无用的invoker，这是为了提高调用的成功率
            if (i > 0) {

                // 状态校验
                checkWhetherDestroyed();

                // 重新获取copyInvokers
                copyInvokers = list(invocation);

                // 校验copyInvokers的状态，为空会抛出异常
                // check again
                checkInvokers(copyInvokers, invocation);
            }

            // 根据负载均衡获取一个可调用的invoker
            Invoker<T> invoker = select(loadbalance, invocation, copyInvokers, invoked);

            // 添加到invoked集合中，当前集合会在select方法中用于标识已经被使用过的invoker
            invoked.add(invoker);

            // 在消费者上下文记录已经调用过的invoker集合
            RpcContext.getContext().setInvokers((List) invoked);
            try {

                // 对invoker进行调用：类型是：RegistryDirectory.InvokerDelegate，但是走的是父类InvokerWrapper.invoke
                // 最终走到：FilterNode.invoke
                Result result = invoker.invoke(invocation);

                // 到这里说明当前调用成功，如果le不等于空，说明上一次尝试有异常，需要打印出警告日志
                if (le != null && logger.isWarnEnabled()) {
                    logger.warn("Although retry the method " + methodName
                            + " in the service " + getInterface().getName()
                            + " was successful by the provider " + invoker.getUrl().getAddress()
                            + ", but there have been failed providers " + providers
                            + " (" + providers.size() + "/" + copyInvokers.size()
                            + ") from the registry " + directory.getUrl().getAddress()
                            + " on the consumer " + NetUtils.getLocalHost()
                            + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                            + le.getMessage(), le);
                }

                // 返回调用结果
                return result;
            } catch (RpcException e) {

                // 业务异常不再重试，直接对外抛出
                // biz exception.
                if (e.isBiz()) {
                    throw e;
                }

                // 非业务异常，赋值给le
                le = e;
            } catch (Throwable e) {

                // 非rpc异常，包装并赋值给le
                le = new RpcException(e.getMessage(), e);
            } finally {

                // 记录本次调用的provider的地址，用于失败场景下，后续调用成功时的警告日志输出
                providers.add(invoker.getUrl().getAddress());
            }
        }
        throw new RpcException(le.getCode(),
            "Failed to invoke the method " + methodName + " in the service " + getInterface().getName() + ". Tried "
                + len + " times of the providers " + providers + " (" + providers.size() + "/" + copyInvokers.size()
                + ") from the registry " + directory.getUrl().getAddress() + " on the consumer "
                + NetUtils.getLocalHost() + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                + le.getMessage(), le.getCause() != null ? le.getCause() : le);
    }

    /**
     * 优先使用{@link RpcContext#getObjectAttachment(java.lang.String)}上的重试配置，如果没有或配置错误，则使用
     * {@link URL#getMethodParameter(java.lang.String, java.lang.String, int)}上的配置，默认为
     * {@link CommonConstants#DEFAULT_RETRIES}，不可小于{@code 1}
     */
    private int calculateInvokeTimes(String methodName) {

        // 从消费者url上获取配置的重试次数，默认为2次，加上实际的调用，有3次调用
        int len = getUrl().getMethodParameter(methodName, RETRIES_KEY, DEFAULT_RETRIES) + 1;

        // 获取消费上下文
        RpcContext rpcContext = RpcContext.getContext();

        // 获取消费上下文中的重试次数，如果不为空，则将当前len修改为上下文重试次数 + 1
        Object retry = rpcContext.getObjectAttachment(RETRIES_KEY);
        if (null != retry && retry instanceof Number) {
            len = ((Number) retry).intValue() + 1;
            rpcContext.removeAttachment(RETRIES_KEY);
        }

        // 当前重试次数最小为1
        if (len <= 0) {
            len = 1;
        }

        return len;
    }

}
