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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.URL;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * URL statistics. (API, Cached, ThreadSafe)
 *
 * @see org.apache.dubbo.rpc.filter.ActiveLimitFilter
 * @see org.apache.dubbo.rpc.filter.ExecuteLimitFilter
 * @see org.apache.dubbo.rpc.cluster.loadbalance.LeastActiveLoadBalance
 */
public class RpcStatus {

    /**
     * 记录服务和服务的{@link RpcStatus}的映射
     * <p>
     * <{@link URL#toIdentityString()}, {@link RpcStatus}>
     */
    private static final ConcurrentMap<String, RpcStatus> SERVICE_STATISTICS =
        new ConcurrentHashMap<String, RpcStatus>();

    /**
     * 记录服务下对应方法和{@link RpcStatus}的映射
     * <p>
     * <{@link URL#toIdentityString()}, <{@link Invocation#getMethodName()}, {@link RpcStatus}>>
     */
    private static final ConcurrentMap<String, ConcurrentMap<String, RpcStatus>> METHOD_STATISTICS =
        new ConcurrentHashMap<String, ConcurrentMap<String, RpcStatus>>();

    private final ConcurrentMap<String, Object> values = new ConcurrentHashMap<String, Object>();

    /**
     * 当前并发度
     */
    private final AtomicInteger active = new AtomicInteger();

    /**
     * 总调用次数
     */
    private final AtomicLong total = new AtomicLong();

    /**
     * 失败调用总数
     */
    private final AtomicInteger failed = new AtomicInteger();

    /**
     * 总调用耗时
     */
    private final AtomicLong totalElapsed = new AtomicLong();

    /**
     * 失败调用总耗时
     */
    private final AtomicLong failedElapsed = new AtomicLong();

    /**
     * 调用最大耗时
     */
    private final AtomicLong maxElapsed = new AtomicLong();

    /**
     * 失败调用最大耗时
     */
    private final AtomicLong failedMaxElapsed = new AtomicLong();

    /**
     * 成功调用最大耗时
     */
    private final AtomicLong succeededMaxElapsed = new AtomicLong();

    private RpcStatus() {
    }

    /**
     * 获取provider服务端的{@link RpcStatus}
     */
    public static RpcStatus getStatus(URL url) {
        String uri = url.toIdentityString();
        return SERVICE_STATISTICS.computeIfAbsent(uri, key -> new RpcStatus());
    }

    /**
     * @param url
     */
    public static void removeStatus(URL url) {
        String uri = url.toIdentityString();
        SERVICE_STATISTICS.remove(uri);
    }

    /**
     * 获取provider服务端对应{@code methodName}的{@link RpcStatus}
     */
    public static RpcStatus getStatus(URL url, String methodName) {
        String uri = url.toIdentityString();
        ConcurrentMap<String, RpcStatus> map = METHOD_STATISTICS.computeIfAbsent(uri, k -> new ConcurrentHashMap<>());
        return map.computeIfAbsent(methodName, k -> new RpcStatus());
    }

    /**
     * @param url
     */
    public static void removeStatus(URL url, String methodName) {
        String uri = url.toIdentityString();
        ConcurrentMap<String, RpcStatus> map = METHOD_STATISTICS.get(uri);
        if (map != null) {
            map.remove(methodName);
        }
    }

    public static void beginCount(URL url, String methodName) {
        beginCount(url, methodName, Integer.MAX_VALUE);
    }

    /**
     * @param url
     */
    public static boolean beginCount(URL url, String methodName, int max) {

        // 判断最大并发度，如果原max小于等于0，则认为不限制并发度，所以设置最大并发度为Integer.MAX_VALUE
        max = (max <= 0) ? Integer.MAX_VALUE : max;

        // 获取provider服务的RpcStatus
        RpcStatus appStatus = getStatus(url);

        // 获取对应方法的RpcStatus
        RpcStatus methodStatus = getStatus(url, methodName);

        // 如果当前方法并发度已经等于Integer.MAX_VALUE，则直接返回false，无需处理，因为Integer.MAX_VALUE是最大并发度
        if (methodStatus.active.get() == Integer.MAX_VALUE) {
            return false;
        }

        // 否则循环增加并发度，之所以在这里循环，是因为methodStatus.active.compareAndSet的操作，避免并发冲突
        for (int i; ; ) {

            // 获取当前方法并发度
            i = methodStatus.active.get();

            // 如果当前方法并发度是Integer.MAX_VALUE，或当前方法并发度+1大于最大并发度，则直接返回false，因为已经达到最大并发度
            if (i == Integer.MAX_VALUE || i + 1 > max) {
                return false;
            }

            // 原子变更方法并发度，如果变更成功，则直接中断循环，如果变更失败，则继续循环判断并发度
            if (methodStatus.active.compareAndSet(i, i + 1)) {
                break;
            }
        }

        // 对应服务的并发度加一，服务并发度不限制
        appStatus.active.incrementAndGet();

        // 返回true，代表并发度变更成功
        return true;
    }

    /**
     * @param url
     * @param elapsed
     * @param succeeded
     */
    public static void endCount(URL url, String methodName, long elapsed, boolean succeeded) {

        // 减少provider服务的并发度
        endCount(getStatus(url), elapsed, succeeded);

        // 减少对应方法的并发去
        endCount(getStatus(url, methodName), elapsed, succeeded);
    }

    private static void endCount(RpcStatus status, long elapsed, boolean succeeded) {

        // 减少并发度
        status.active.decrementAndGet();

        // 增加总调用记录
        status.total.incrementAndGet();

        // 增加总调用耗时记录
        status.totalElapsed.addAndGet(elapsed);

        // 判断最大调用耗时
        if (status.maxElapsed.get() < elapsed) {
            status.maxElapsed.set(elapsed);
        }

        // 调用成功则判断最大调用成功耗时
        if (succeeded) {
            if (status.succeededMaxElapsed.get() < elapsed) {
                status.succeededMaxElapsed.set(elapsed);
            }

        }

        // 调用失败则增加失败调用计数和失败调用总耗时，并计算最大失败调用耗时
        else {
            status.failed.incrementAndGet();
            status.failedElapsed.addAndGet(elapsed);
            if (status.failedMaxElapsed.get() < elapsed) {
                status.failedMaxElapsed.set(elapsed);
            }
        }
    }

    /**
     * set value.
     *
     * @param key
     * @param value
     */
    public void set(String key, Object value) {
        values.put(key, value);
    }

    /**
     * get value.
     *
     * @param key
     * @return value
     */
    public Object get(String key) {
        return values.get(key);
    }

    /**
     * get active.
     *
     * @return active
     */
    public int getActive() {
        return active.get();
    }

    /**
     * get total.
     *
     * @return total
     */
    public long getTotal() {
        return total.longValue();
    }

    /**
     * get total elapsed.
     *
     * @return total elapsed
     */
    public long getTotalElapsed() {
        return totalElapsed.get();
    }

    /**
     * get average elapsed.
     *
     * @return average elapsed
     */
    public long getAverageElapsed() {
        long total = getTotal();
        if (total == 0) {
            return 0;
        }
        return getTotalElapsed() / total;
    }

    /**
     * get max elapsed.
     *
     * @return max elapsed
     */
    public long getMaxElapsed() {
        return maxElapsed.get();
    }

    /**
     * get failed.
     *
     * @return failed
     */
    public int getFailed() {
        return failed.get();
    }

    /**
     * get failed elapsed.
     *
     * @return failed elapsed
     */
    public long getFailedElapsed() {
        return failedElapsed.get();
    }

    /**
     * get failed average elapsed.
     *
     * @return failed average elapsed
     */
    public long getFailedAverageElapsed() {
        long failed = getFailed();
        if (failed == 0) {
            return 0;
        }
        return getFailedElapsed() / failed;
    }

    /**
     * get failed max elapsed.
     *
     * @return failed max elapsed
     */
    public long getFailedMaxElapsed() {
        return failedMaxElapsed.get();
    }

    /**
     * get succeeded.
     *
     * @return succeeded
     */
    public long getSucceeded() {
        return getTotal() - getFailed();
    }

    /**
     * get succeeded elapsed.
     *
     * @return succeeded elapsed
     */
    public long getSucceededElapsed() {
        return getTotalElapsed() - getFailedElapsed();
    }

    /**
     * get succeeded average elapsed.
     *
     * @return succeeded average elapsed
     */
    public long getSucceededAverageElapsed() {
        long succeeded = getSucceeded();
        if (succeeded == 0) {
            return 0;
        }
        return getSucceededElapsed() / succeeded;
    }

    /**
     * get succeeded max elapsed.
     *
     * @return succeeded max elapsed.
     */
    public long getSucceededMaxElapsed() {
        return succeededMaxElapsed.get();
    }

    /**
     * Calculate average TPS (Transaction per second).
     *
     * @return tps
     */
    public long getAverageTps() {
        if (getTotalElapsed() >= 1000L) {
            return getTotal() / (getTotalElapsed() / 1000L);
        }
        return getTotal();
    }


}
