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
 * 按照最少活跃请求数（最小并发度）筛选invoker，如果有只有一个invoker，则直接对外返回，如果有多个invoker，进而判断这些invoker的权重，如果权重一致，则根据invoker数量随机出一个，否则根据权重随机出一个
 * <p>
 * 较少活跃请求书（并发度）的invoker当前处理能力较强，所以选择相关的invoker来作为当前请求的提供者
 * <p>
 * 最小活跃数负载均衡算法：它认为当前活跃请求数越小的 Provider 节点，剩余的处理能力越多，处理请求的效率也就越高，那么该 Provider 在单位时间内就可以处理更多的请求，所以我们应该优先将请求分配给该 Provider节点。
 * <p>
 * 但是现在系统大多是集群，而这里的最小活跃度是单机记录的，最终选择出的invoker并不一定处理能力较强
 * <p>
 * LeastActiveLoadBalance
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 *
 * @see RpcStatus
 * @see RpcStatus#getActive()
 * @see org.apache.dubbo.rpc.filter.ActiveLimitFilter
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        int length = invokers.size();

        // 单个invoker当前时刻的最少活跃请求数（最小并发度）
        // The least active value of all invokers
        int leastActive = -1;

        // 相同最少活跃请求数（最小并发度）的invoker数量
        // The number of invokers having the same least active value (leastActive)
        int leastCount = 0;

        // 用于记录最少活跃请求数的invoker在invokers集合中的索引
        // The index of invokers having the same least active value (leastActive)
        int[] leastIndexes = new int[length];

        // 所有invoker的权重
        // the weight of every invokers
        int[] weights = new int[length];

        // 最少活跃请求数的invoker的权重总和
        // The sum of the warmup weights of all the least active invokers
        int totalWeight = 0;

        // 第一个有最少活跃请求数的invoker的权重
        // The weight of the first least active invoker
        int firstWeight = 0;

        // 所有拥有最少活跃请求数的invoker，权重是否一致
        // Every least active invoker has the same weight value?
        boolean sameWeight = true;

        // Filter out all the least active invokers
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);

            // 获取当前invoker的活跃请求
            // Get the active number of the invoker
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();

            // 计算invoker的权重
            // Get the weight of the invoker's configuration. The default value is 100.
            int afterWarmup = getWeight(invoker, invocation);

            // 记录权重
            // save for later use
            weights[i] = afterWarmup;

            // 如果最小活跃度等于-1（刚开始循环），或当前invoker的活跃度更小，则更新记录
            // If it is the first invoker or the active number of the invoker is less than the current least active number
            if (leastActive == -1 || active < leastActive) {

                // 当前活跃请求书作为最少活跃请求数
                // Reset the active number of the current invoker to the least active number
                leastActive = active;

                // 重置最少活跃请求书的invoker数量
                // Reset the number of least active invokers
                leastCount = 1;

                // 重新开始记录拥有最少活跃请求数的invoker在invokers集合中的索引
                // Put the first least active invoker first in leastIndexes
                leastIndexes[0] = i;

                // 重新记录权重
                // Reset totalWeight
                totalWeight = afterWarmup;
                // Record the weight the first least active invoker
                firstWeight = afterWarmup;

                // 重置相同权重标识
                // Each invoke has the same weight (only one invoker here)
                sameWeight = true;
                // If current invoker's active value equals with leaseActive, then accumulating.
            }

            // 如果当前活跃请求数等于之前的最少活跃请求数，说明当前invoker拥一样拥有最少活跃请求书，则进行相关的记录和计算
            else if (active == leastActive) {
                // Record the index of the least active invoker in leastIndexes order
                leastIndexes[leastCount++] = i;
                // Accumulate the total weight of the least active invoker
                totalWeight += afterWarmup;
                // If every invoker has the same weight?
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }

        // 如果拥有最少活跃请求数的invoker只有一个，则直接返回
        // Choose an invoker from all the least active invokers
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker directly.
            return invokers.get(leastIndexes[0]);
        }

        // 以下这部分逻辑和RandomLoadBalance一致

        // 拥有最少活跃请求数的invoker有多个
        // 这些invoker的权重不一样，且总权重大于0，则通过随机出一个权重值来获取一个invoker
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on 
            // totalWeight.
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }

        // 否则根据拥有最少活跃请求数的invoker数量，来随机出一个invoker
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
