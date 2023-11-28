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

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.Merger;
import org.apache.dubbo.rpc.cluster.merger.MergerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.MERGER_KEY;

/**
 * 按照配置调用多个invoker，并将结果合并返回
 */
@SuppressWarnings("unchecked")
public class MergeableClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger log = LoggerFactory.getLogger(MergeableClusterInvoker.class);

    public MergeableClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {

        // 校验invokers
        checkInvokers(invokers, invocation);

        // 获取配置的合并器merger的key
        String merger = getUrl().getMethodParameter(invocation.getMethodName(), MERGER_KEY);

        // 如果没有配置merger参数，则循环invokers集合，获取到第一个可用的invoker发起请求即可
        // If a method doesn't have a merger, only invoke one Group
        if (ConfigUtils.isEmpty(merger)) {
            for (final Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    try {
                        return invoker.invoke(invocation);
                    } catch (RpcException e) {

                        // invoker是在被筛选出来之后失效的，这种情况打印日志后，继续循环，获取下一个可用的invoker
                        if (e.isNoInvokerAvailableAfterFilter()) {
                            log.debug(
                                "No available provider for service" + getUrl().getServiceKey() + " on group " + invoker
                                    .getUrl()
                                    .getParameter(GROUP_KEY) + ", will continue to try another group.");
                        } else {
                            throw e;
                        }
                    }
                }
            }

            // 如果循环结束都没调用成功，这种情况直接获取第一个invoker，无需判断直接返回
            // todo 为什么用迭代器，而不是使用get(0)？上面的forEach循环底层也是用迭代器，两者会有什么关联吗？完整了解forEach底层迭代器机制
            return invokers.iterator().next().invoke(invocation);
        }

        // 获取返回值类型
        Class<?> returnType;
        try {
            returnType = getInterface().getMethod(
                    invocation.getMethodName(), invocation.getParameterTypes()).getReturnType();
        } catch (NoSuchMethodException e) {
            returnType = null;
        }

        // 循环invokers，调用方式为异步，并将请求结果记录在results中
        Map<String, Result> results = new HashMap<>();
        for (final Invoker<T> invoker : invokers) {
            RpcInvocation subInvocation = new RpcInvocation(invocation, invoker);
            subInvocation.setAttachment(ASYNC_KEY, "true");
            results.put(invoker.getUrl().getServiceKey(), invoker.invoke(subInvocation));
        }

        Object result = null;

        List<Result> resultList = new ArrayList<Result>(results.size());

        // 循环存储请求结果的results集合，阻塞获取请求结果，添加到集合中，请求如果有异常，只会打印错误日志
        for (Map.Entry<String, Result> entry : results.entrySet()) {
            Result asyncResult = entry.getValue();
            try {
                Result r = asyncResult.get();
                if (r.hasException()) {
                    log.error("Invoke " + getGroupDescFromServiceKey(entry.getKey()) +
                                    " failed: " + r.getException().getMessage(),
                            r.getException());
                } else {
                    resultList.add(r);
                }
            } catch (Exception e) {
                throw new RpcException("Failed to invoke service " + entry.getKey() + ": " + e.getMessage(), e);
            }
        }

        // 请求结果为空，或只有一个的处理
        if (resultList.isEmpty()) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        } else if (resultList.size() == 1) {
            return AsyncRpcResult.newDefaultAsyncResult(resultList.get(0).getValue(), invocation);
        }

        // 无返回值的处理
        if (returnType == void.class) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        }

        // 如果merger是以点开头，后面携带的字符则是返回值中的方法名称，需要从返回值类对象中获取对应方法，并进行执行
        if (merger.startsWith(".")) {

            // 获取对应的方法对象
            merger = merger.substring(1);
            Method method;
            try {
                method = returnType.getMethod(merger, returnType);
            } catch (NoSuchMethodException e) {
                throw new RpcException(
                    "Can not merge result because missing method [ " + merger + " ] in class [ " + returnType.getName()
                        + " ]");
            }

            // 反射设置可访问
            ReflectUtils.makeAccessible(method);

            // 获取第一个invoker调用返回值，是一个Result
            result = resultList.remove(0).getValue();
            try {

                // 如果对应方法的返回值类型不是void，且方法的返回值类型是Result的类型/子类，说明是将作为invoker调用结果的Result传入，处理后返回出一个新的其他类型的result
                if (method.getReturnType() != void.class && method
                    .getReturnType()
                    .isAssignableFrom(result.getClass())) {

                    // 这里其实可以明确，对应的方法一定是Result的子类，因为此处第一个发生调用的result，一定是Result类型
                    // 循环result集合，调用方法，进行结果合并
                    for (Result r : resultList) {
                        result = method.invoke(result, r.getValue());
                    }
                }

                // 否则说明只是要将result传入对应的方法进行处理，这种情况下，最终返回的result一定是最终那个
                else {
                    for (Result r : resultList) {
                        method.invoke(result, r.getValue());
                    }
                }
            } catch (Exception e) {
                throw new RpcException("Can not merge result: " + e.getMessage(), e);
            }
        }

        // merger不是以点开头，那么就代表具体的合并器，获取到对应的合并器，并进行结果合并
        else {
            Merger resultMerger;

            // 如果merger是默认配置，那么根据方法返回值类型动态获取对应的合并器
            if (ConfigUtils.isDefault(merger)) {
                resultMerger = MergerFactory.getMerger(returnType);
            }

            // 否则根据merger配置获取对应的合并器
            else {
                resultMerger = ExtensionLoader.getExtensionLoader(Merger.class).getExtension(merger);
            }

            // 合并器不为空的情况下，合并请求结果
            if (resultMerger != null) {

                // 从List<Result>的集合中提取出实际的调用结果，添加到rets集合中
                List<Object> rets = new ArrayList<Object>(resultList.size());
                for (Result r : resultList) {
                    rets.add(r.getValue());
                }

                // 调用合并器，合并结果
                result = resultMerger.merge(rets.toArray((Object[])Array.newInstance(returnType, 0)));
            } else {
                throw new RpcException("There is no merger to merge result.");
            }
        }
        return AsyncRpcResult.newDefaultAsyncResult(result, invocation);
    }


    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public boolean isAvailable() {
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        directory.destroy();
    }

    private String getGroupDescFromServiceKey(String key) {
        int index = key.indexOf("/");
        if (index > 0) {
            return "group [ " + key.substring(0, index) + " ]";
        }
        return key;
    }
}
