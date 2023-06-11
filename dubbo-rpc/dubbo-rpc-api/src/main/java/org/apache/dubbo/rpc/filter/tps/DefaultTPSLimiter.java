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
package org.apache.dubbo.rpc.filter.tps;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.rpc.Constants.DEFAULT_TPS_LIMIT_INTERVAL;
import static org.apache.dubbo.rpc.Constants.TPS_LIMIT_INTERVAL_KEY;
import static org.apache.dubbo.rpc.Constants.TPS_LIMIT_RATE_KEY;

/**
 * 默认tps限制器实现
 * <p>
 * DefaultTPSLimiter is a default implementation for tps filter. It is an in memory based implementation for storing
 * tps information. It internally use
 *
 * @see org.apache.dubbo.rpc.filter.TpsLimitFilter
 */
public class DefaultTPSLimiter implements TPSLimiter {

    /**
     * key为serviceKey，value为限制器数据
     * <p>
     * key是serviceKey，代表tps限制是接口级别的
     */
    private final ConcurrentMap<String, StatItem> stats = new ConcurrentHashMap<String, StatItem>();

    @Override
    public boolean isAllowable(URL url, Invocation invocation) {

        // 获取tps限制，小于等于0代表不限制
        int rate = url.getParameter(TPS_LIMIT_RATE_KEY, -1);

        // 获取tps限制的间隔，默认是60秒
        long interval = url.getParameter(TPS_LIMIT_INTERVAL_KEY, DEFAULT_TPS_LIMIT_INTERVAL);

        // 获取serviceKey
        String serviceKey = url.getServiceKey();

        // 如果rate大于0，说明要限制tps
        if (rate > 0) {

            // 获取StatItem，并发数据状态项
            StatItem statItem = stats.get(serviceKey);

            // 如果为空，则创建一个
            if (statItem == null) {
                stats.putIfAbsent(serviceKey, new StatItem(serviceKey, rate, interval));
                statItem = stats.get(serviceKey);
            }

            // 如果不为空，则校验并发度rate和并发间隔interval是否一致，如果不一致则需要重新创建一个
            else {
                //rate or interval has changed, rebuild
                if (statItem.getRate() != rate || statItem.getInterval() != interval) {
                    stats.put(serviceKey, new StatItem(serviceKey, rate, interval));
                    statItem = stats.get(serviceKey);
                }
            }

            // 判断并发度是否达到上限
            return statItem.isAllowable();
        }

        // 如果rate小于等于0，则无需限制tps，如果存在对应的StatItem，还需要移除
        else {
            StatItem statItem = stats.get(serviceKey);
            if (statItem != null) {
                stats.remove(serviceKey);
            }
        }

        return true;
    }

}
