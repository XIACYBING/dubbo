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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ShortestResponseLoadBalance
 * </p>
 * Filter the number of invokers with the shortest response time of success calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 * <p>
 * 最快响应负载均衡：从多个 Provider 节点中选出调用成功的且响应时间最短的 Provider 节点，不过满足该条件的 Provider 节点可能有多个，所以还要再使用随机算法进行一次选择，得到最终要调用的 Provider 节点。
 * <p>
 * 当前算法和{@link LeastActiveLoadBalance}很像，不同的是前者基于{@link RpcStatus#getActive() 当前活跃请求数}，而当前负载均衡基于
 * {@link RpcStatus#getSucceededAverageElapsed() 平均耗时}和{@link RpcStatus#getActive() 当前活跃请求数}
 *
 * @see LeastActiveLoadBalance
 * @see RpcStatus
 * @see RpcStatus#getActive()
 * @see RpcStatus#getSucceededAverageElapsed()
 * @see org.apache.dubbo.rpc.filter.ActiveLimitFilter
 */
public class ShortestResponseLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "shortestresponse";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        int length = invokers.size();

        // 最小响应耗时总和
        // Estimated shortest response time of all invokers
        long shortestResponse = Long.MAX_VALUE;

        // 拥有最小响应耗时的invoker的对象数量
        // The number of invokers having the same estimated shortest response time
        int shortestCount = 0;

        // 拥有最小响应耗时的invoker在invokers集合中的索引
        // The index of invokers having the same estimated shortest response time
        int[] shortestIndexes = new int[length];

        // 所有invoker的权重记录
        // the weight of every invokers
        int[] weights = new int[length];

        // 拥有最小响应耗时的invoker的权重综合
        // The sum of the warmup weights of all the shortest response  invokers
        int totalWeight = 0;

        // 当前invokers集合中第一个拥有最小响应耗时的invoker的权重
        // The weight of the first shortest response invokers
        int firstWeight = 0;

        // 拥有最小响应耗时的invoker的权重是否一致
        // Every shortest response invoker has the same weight value?
        boolean sameWeight = true;

        // 循环invokers，将拥有最小响应耗时的invoker都筛选出来
        // Filter out all the shortest response invokers
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());

            // 获取当前invoker目前所有的成功调用的平均耗时
            // Calculate the estimated response time from the product of active connections and succeeded average elapsed time.
            long succeededAverageElapsed = rpcStatus.getSucceededAverageElapsed();

            // 当前invoker的活跃请求数（并发请求），加上当前请求
            int active = rpcStatus.getActive() + 1;

            // 基于平均耗时，计算当前invoker处理完所有请求的耗时总和
            long estimateResponse = succeededAverageElapsed * active;

            // 当前invoker的权重
            int afterWarmup = getWeight(invoker, invocation);

            // 记录权重
            weights[i] = afterWarmup;

            // 当前invoker处理完成的耗时总和小于之前invoker的最小耗时总和，则记录当前invoker的相关信息，并重置相关标识
            // Same as LeastActiveLoadBalance
            if (estimateResponse < shortestResponse) {

                // 当前耗时总和作为最小耗时总和
                shortestResponse = estimateResponse;

                // 重置拥有最小耗时总和的invoker数量、索引和相关权重信息
                shortestCount = 1;
                shortestIndexes[0] = i;
                totalWeight = afterWarmup;
                firstWeight = afterWarmup;
                sameWeight = true;
            }

            // 当前invoker的耗时总和和之前最小耗时总和一样，则记录相关信息
            else if (estimateResponse == shortestResponse) {
                shortestIndexes[shortestCount++] = i;
                totalWeight += afterWarmup;
                if (sameWeight && i > 0 && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }

        // 如果拥有最小耗时总和的invoker只有一个，则直接返回
        if (shortestCount == 1) {
            return invokers.get(shortestIndexes[0]);
        }

        // 否则需要根据权重随机出一个invoker

        // 如果拥有最小耗时总和的invoker权重不一致，且总权重大于0，则需要随机出一个权重值，判断随机权重值落在哪个区间，返回对应区间的invoker
        if (!sameWeight && totalWeight > 0) {
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < shortestCount; i++) {
                int shortestIndex = shortestIndexes[i];
                offsetWeight -= weights[shortestIndex];
                if (offsetWeight < 0) {
                    return invokers.get(shortestIndex);
                }
            }
        }

        // 否则根据拥有最小权重总和的invoker数量，随机出一个invoker并返回
        return invokers.get(shortestIndexes[ThreadLocalRandom.current().nextInt(shortestCount)]);
    }

}
