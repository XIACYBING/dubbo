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
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WARMUP;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WEIGHT;
import static org.apache.dubbo.rpc.cluster.Constants.WARMUP_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

/**
 * AbstractLoadBalance
 */
public abstract class AbstractLoadBalance implements LoadBalance {

    /**
     * Calculate the weight according to the uptime proportion of warmup time
     * the new weight will be within 1(inclusive) to weight(inclusive)
     *
     * @param uptime the uptime in milliseconds
     * @param warmup the warmup time in milliseconds
     * @param weight the weight of an invoker
     * @return weight which takes warmup into account
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {

        // 根据启动毫秒数、预热毫秒数和原权重，计算出一个适合当前情况的权重
        // 随着启动毫秒数uptime的增大，权重会慢慢变大，逐渐大于1，大于1之后就是采用原权重了
        int ww = (int)(uptime / ((float)warmup / weight));

        // 如果权重小于1，则返回1（1是最小有效权重），否则原权重和新权重，两者取小
        return ww < 1 ? 1 : (Math.min(ww, weight));
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {

        // 提供者集合为空，直接返回null
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }

        // 只有一个，直接返回这个
        if (invokers.size() == 1) {
            return invokers.get(0);
        }

        // 否则执行子类的负载均衡算法，获取到一个提供者
        return doSelect(invokers, url, invocation);
    }

    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);


    /**
     * Get the weight of the invoker's invocation which takes warmup time into account
     * if the uptime is within the warmup time, the weight will be reduce proportionally
     *
     * @param invoker    the invoker
     * @param invocation the invocation of this invoker
     * @return weight
     */
    int getWeight(Invoker<?> invoker, Invocation invocation) {
        int weight;
        URL url = invoker.getUrl();

        // 如果是RegistryService的url，则直接从url上获取权重
        // 多注册中心的场景，在多个注册中心中进行负载    todo 不清楚是啥场景
        // Multiple registry scenario, load balance among multiple registries.
        if (REGISTRY_SERVICE_REFERENCE_PATH.equals(url.getServiceInterface())) {
            weight = url.getParameter(REGISTRY_KEY + "." + WEIGHT_KEY, DEFAULT_WEIGHT);
        }

        // 否则进行权重计算
        else {

            // 获取对应方法下的权重配置 todo 应该支持每个方法级别的权重配置
            weight = url.getMethodParameter(invocation.getMethodName(), WEIGHT_KEY, DEFAULT_WEIGHT);
            if (weight > 0) {

                // 获取提供者的启动时间
                long timestamp = invoker.getUrl().getParameter(TIMESTAMP_KEY, 0L);

                // 启动时间大于0：正常配置
                if (timestamp > 0L) {

                    // 获取已启动的毫秒数
                    long uptime = System.currentTimeMillis() - timestamp;

                    // 已启动时间小于0，说明消费者和提供者的机器时间不一致，返回最低有效权重1
                    if (uptime < 0) {
                        return 1;
                    }

                    // 获取提供者配置的预热时间，默认是10分钟
                    int warmup = invoker.getUrl().getParameter(WARMUP_KEY, DEFAULT_WARMUP);

                    // 启动时间大于0，但是小于预热时间，则需要根据启动时间、预热时间和原权重计算出一个新权重
                    if (uptime > 0 && uptime < warmup) {
                        weight = calculateWarmupWeight((int)uptime, warmup, weight);
                    }
                }
            }
        }
        return Math.max(weight, 0);
    }
}
