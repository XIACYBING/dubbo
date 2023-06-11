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

import java.util.concurrent.atomic.LongAdder;

/**
 * 判断某个请求是否应该被允许
 * <p>
 * Judge whether a particular invocation of service provider method should be allowed within a configured time interval.
 * As a state it contain name of key ( e.g. method), last invocation time, interval and rate count.
 */
class StatItem {

    /**
     * 标识符，一般是serviceKey
     */
    private String name;

    /**
     * 最后一次重置时间
     */
    private long lastResetTime;

    /**
     * 并发间隔
     */
    private long interval;

    /**
     * 令牌器，用于发放允许请求的令牌
     */
    private LongAdder token;

    /**
     * 并发度，每{@link #interval}毫秒可以发放多少令牌
     */
    private int rate;

    StatItem(String name, int rate, long interval) {
        this.name = name;
        this.rate = rate;
        this.interval = interval;
        this.lastResetTime = System.currentTimeMillis();
        this.token = buildLongAdder(rate);
    }

    public boolean isAllowable() {

        // 获取当前毫秒
        long now = System.currentTimeMillis();

        // 如果当前毫秒数已经超过一轮并发间隔，则重新初始化一个令牌发放器，并设置最后重置时间为当前毫秒数
        if (now > lastResetTime + interval) {
            token = buildLongAdder(rate);
            lastResetTime = now;
        }

        // 如果当前令牌数量小于等于0，则不允许请求通过
        if (token.sum() <= 0) {
            return false;
        }

        // 令牌数量减一
        token.decrement();

        // 允许请求通过
        return true;
    }

    public long getInterval() {
        return interval;
    }


    public int getRate() {
        return rate;
    }


    long getLastResetTime() {
        return lastResetTime;
    }

    long getToken() {
        return token.sum();
    }

    @Override
    public String toString() {
        return new StringBuilder(32).append("StatItem ")
                                    .append("[name=")
                                    .append(name)
                                    .append(", ")
                                    .append("rate = ")
                                    .append(rate)
                                    .append(", ")
                                    .append("interval = ")
                                    .append(interval)
                                    .append("]")
                                    .toString();
    }

    /**
     * 初始化一个令牌器
     */
    private LongAdder buildLongAdder(int rate) {
        LongAdder adder = new LongAdder();
        adder.add(rate);
        return adder;
    }

}
