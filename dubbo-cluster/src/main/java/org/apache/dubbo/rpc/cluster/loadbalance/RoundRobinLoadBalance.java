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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance.
 * <p>
 * 轮询是一种无状态负载均衡算法，实现简单，适用于集群中所有 Provider 节点性能相近的场景。
 * 但现实情况中就很难保证这一点了，因为很容易出现集群中性能最好和最差的 Provider 节点处理同样流量的情况，这就可能导致性能差的 Provider 节点各方面资源非常紧张，
 * 甚至无法及时响应了，但是性能好的 Provider 节点的各方面资源使用还较为空闲。这时我们可以通过加权轮询的方式，降低分配到性能较差的 Provider 节点的流量。
 *
 * <ol>
 *     每个 Provider 节点有两个权重：一个权重是配置的 {@link WeightedRoundRobin#weight}，该值在负载均衡的过程中不会变化；另一个权重是
 *     {@link WeightedRoundRobin#current}，该值会在负载均衡的过程中动态调整，初始值为 0。
 *     当有新的请求进来时，RoundRobinLoadBalance 会遍历 Invoker 列表，并用对应的 currentWeight 加上其配置的权重（{@link WeightedRoundRobin#increaseCurrent()}）。
 *     遍历完成后，再找到最大的currentWeight，将其减去权重总和，然后返回相应的 Invoker 对象。
 * <p>
 *     下面我们通过一个示例说明 RoundRobinLoadBalance 的执行流程，这里我们依旧假设 A、B、C 三个invoker的权重比例为 5:1:1。
 *
 * <li>处理第一个请求，currentWeight 数组中的权重与配置的 weight 相加，即从 [0, 0, 0] 变为 [5, 1, 1]。接下来，从中选择权重最大的 Invoker 作为结果，即节点 A。
 * 最后，将节点 A 的 currentWeight 值减去 totalWeight 值，最终得到 currentWeight 数组为 [-2, 1, 1]。</li>
 *
 * <li>处理第二个请求，currentWeight 数组中的权重与配置的 weight 相加，即从 [-2, 1, 1] 变为 [3, 2, 2]。接下来，从中选择权重最大的 Invoker 作为结果，即节点 A。
 * 最后，将节点 A 的 currentWeight 值减去 totalWeight 值，最终得到 currentWeight 数组为 [-4, 2, 2]。</li>
 *
 * <li>处理第三个请求，currentWeight 数组中的权重与配置的 weight 相加，即从 [-4, 2, 2] 变为 [1, 3, 3]。接下来，从中选择权重最大的 Invoker 作为结果，即节点 B。
 * 最后，将节点 B 的 currentWeight 值减去 totalWeight 值，最终得到 currentWeight 数组为 [1, -4, 3]。</li>
 *
 * <li>处理第四个请求，currentWeight 数组中的权重与配置的 weight 相加，即从 [1, -4, 3] 变为 [6, -3, 4]。接下来，从中选择权重最大的 Invoker 作为结果，即节点 A。
 * 最后，将节点 A 的 currentWeight 值减去 totalWeight 值，最终得到 currentWeight 数组为 [-1, -3, 4]。</li>
 *
 * <li>处理第五个请求，currentWeight 数组中的权重与配置的 weight 相加，即从 [-1, -3, 4] 变为 [4, -2, 5]。接下来，从中选择权重最大的 Invoker 作为结果，即节点 C。
 * 最后，将节点 C 的 currentWeight 值减去 totalWeight 值，最终得到 currentWeight 数组为 [4, -2, -2]。</li>
 *
 * <li>处理第六个请求，currentWeight 数组中的权重与配置的 weight 相加，即从 [4, -2, -2] 变为 [9, -1, -1]。接下来，从中选择权重最大的 Invoker 作为结果，即节点 A。
 * 最后，将节点 A 的 currentWeight 值减去 totalWeight 值，最终得到 currentWeight 数组为 [2, -1, -1]。</li>
 *
 * <li>处理第七个请求，currentWeight 数组中的权重与配置的 weight 相加，即从 [2, -1, -1] 变为 [7, 0, 0]。接下来，从中选择权重最大的 Invoker 作为结果，即节点 A。
 * 最后，将节点 A 的 currentWeight 值减去 totalWeight 值，最终得到 currentWeight 数组为 [0, 0, 0]。</li>
 * </ol>
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";

    /**
     * invoker清理周期，60秒
     */
    private static final int RECYCLE_PERIOD = 60000;

    protected static class WeightedRoundRobin {

        /**
         * 对应invoker的配置权重
         */
        private int weight;

        /**
         * 当前的总权重
         */
        private AtomicLong current = new AtomicLong(0);

        /**
         * 当前的总权重被更新的时间
         */
        private long lastUpdate;

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }

        public long increaseCurrent() {
            return current.addAndGet(weight);
        }

        public void sel(int total) {
            current.addAndGet(-1 * total);
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

    /**
     * 权重集合：<{group}/{interfaceName}:{version}.{methodName}, <{@link URL#toIdentityString()}, {@link WeightedRoundRobin}>>
     */
    private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap =
        new ConcurrentHashMap<String, ConcurrentMap<String, WeightedRoundRobin>>();

    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     *
     * @param invokers
     * @param invocation
     * @return
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();

        // 根据key获取当前方法对应的，invoker和权重对象的映射集合
        ConcurrentMap<String, WeightedRoundRobin> map =
            methodWeightMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>());

        // 所有invoker的总权重
        int totalWeight = 0;

        // 在循环中使用的，当前最大的权重值
        long maxCurrent = Long.MIN_VALUE;

        // 当前时间
        long now = System.currentTimeMillis();

        // 最终选择的invoker和对应的权重对象
        Invoker<T> selectedInvoker = null;
        WeightedRoundRobin selectedWRR = null;

        // 循环invokers集合，获取当前权重最大的一个invoker
        for (Invoker<T> invoker : invokers) {

            // 获取url的唯一字符串，不带参数：dubbo://IP:PORT/com.xxx.yyy.AService，该字符串在当前场景代表着当前的invoker
            String identifyString = invoker.getUrl().toIdentityString();

            // 获取当前invoker的权重
            int weight = getWeight(invoker, invocation);

            // 获取当前invoker对应的权重对象，如果没有则创建
            WeightedRoundRobin weightedRoundRobin = map.computeIfAbsent(identifyString, k -> {
                WeightedRoundRobin wrr = new WeightedRoundRobin();
                wrr.setWeight(weight);
                return wrr;
            });

            // 如果当前invoker的权重和权重对象中记录的权重不一样，说明权重发生了变化，需要更新
            if (weight != weightedRoundRobin.getWeight()) {
                //weight changed
                weightedRoundRobin.setWeight(weight);
            }

            // 将权重对象中，当前的总权重加上当前invoker的权重，得到当前invoker的最终权重
            long cur = weightedRoundRobin.increaseCurrent();

            // 设置更新时间
            weightedRoundRobin.setLastUpdate(now);

            // 如果当前invoker的最终权重大于之前的最终权重，则将当前invoker作为最终的invoker
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedInvoker = invoker;
                selectedWRR = weightedRoundRobin;
            }

            // 记录总权重
            totalWeight += weight;
        }

        // 如果invokers集合的数量和map集合中的数量不一致，说明有invokers集合发生了变化，需要进行清理
        // 如果某个invoker在60秒内都没有被携带到invokers集合中，则可以移除
        // 可能是invoker对应的提供者下线了，或者被某些路由规则屏蔽了
        if (invokers.size() != map.size()) {
            map.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD);
        }

        // 获取到对应的invoker，不为空则返回
        if (selectedInvoker != null) {
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }

        // 一定会获取到一个invoker，此处仅为兜底，不会执行到
        // should not happen here
        return invokers.get(0);
    }

}
