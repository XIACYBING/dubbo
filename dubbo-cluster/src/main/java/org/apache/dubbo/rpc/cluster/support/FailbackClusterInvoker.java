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

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_FAILBACK_TIMES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FAILBACK_TASKS;
import static org.apache.dubbo.rpc.cluster.Constants.FAIL_BACK_TASKS_KEY;

/**
 * When fails, record failure requests and schedule for retry on a regular interval.
 * Especially useful for services of notification.
 *
 * <a href="http://en.wikipedia.org/wiki/Failback">Failback</a>
 */
public class FailbackClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailbackClusterInvoker.class);

    /**
     * 失败重试周期：秒
     */
    private static final long RETRY_FAILED_PERIOD = 5;

    /**
     * 重试次数
     */
    private final int retries;

    private final int failbackTasks;

    /**
     * 定时任务执行器
     */
    private volatile Timer failTimer;

    public FailbackClusterInvoker(Directory<T> directory) {
        super(directory);

        int retriesConfig = getUrl().getParameter(RETRIES_KEY, DEFAULT_FAILBACK_TIMES);
        if (retriesConfig <= 0) {
            retriesConfig = DEFAULT_FAILBACK_TIMES;
        }
        int failbackTasksConfig = getUrl().getParameter(FAIL_BACK_TASKS_KEY, DEFAULT_FAILBACK_TASKS);
        if (failbackTasksConfig <= 0) {
            failbackTasksConfig = DEFAULT_FAILBACK_TASKS;
        }
        retries = retriesConfig;
        failbackTasks = failbackTasksConfig;
    }

    private void addFailed(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, Invoker<T> lastInvoker) {

        // Double Check，初始化定时任务执行器，初始化32个哈希槽，每个哈希槽代表1秒，定时任务在秒级别执行
        if (failTimer == null) {
            synchronized (this) {
                if (failTimer == null) {
                    failTimer = new HashedWheelTimer(new NamedThreadFactory("failback-cluster-timer", true), 1,
                        TimeUnit.SECONDS, 32, failbackTasks);
                }
            }
        }

        // 生成重试任务，每五秒重试一次
        RetryTimerTask retryTimerTask =
            new RetryTimerTask(loadbalance, invocation, invokers, lastInvoker, retries, RETRY_FAILED_PERIOD);

        // 提交重试任务
        try {
            failTimer.newTimeout(retryTimerTask, RETRY_FAILED_PERIOD, TimeUnit.SECONDS);
        } catch (Throwable e) {
            logger.error(
                "Failback background works error,invocation->" + invocation + ", exception: " + e.getMessage());
        }
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        Invoker<T> invoker = null;
        try {

            // 校验invokers，为空抛出异常
            checkInvokers(invokers, invocation);

            // 获取一个执行请求的invoker
            invoker = select(loadbalance, invocation, invokers, null);

            // 执行请求
            return invoker.invoke(invocation);
        } catch (Throwable e) {
            logger.error("Failback to invoke method " + invocation.getMethodName()
                + ", wait for retry in background. Ignored exception: " + e.getMessage() + ", ", e);

            // 如果异常，添加到定时任务器中
            addFailed(loadbalance, invocation, invokers, invoker);

            // ignore
            return AsyncRpcResult.newDefaultAsyncResult(null, null, invocation);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        if (failTimer != null) {
            failTimer.stop();
        }
    }

    /**
     * RetryTimerTask
     */
    private class RetryTimerTask implements TimerTask {

        /**
         * 请求信息
         */
        private final Invocation invocation;

        /**
         * 负载均衡器
         */
        private final LoadBalance loadbalance;

        /**
         * 异常请求时的invokers
         */
        private final List<Invoker<T>> invokers;

        /**
         * 可重试次数
         */
        private final int retries;

        /**
         * 重试周期
         *
         * @see FailbackClusterInvoker#RETRY_FAILED_PERIOD
         */
        private final long tick;

        /**
         * 异常请求时的invoker，将在{@link AbstractClusterInvoker#select(LoadBalance, Invocation, List, List)}调用时作为已使用的invoker传入
         */
        private Invoker<T> lastInvoker;

        /**
         * 当前重试次数
         */
        private int retryTimes = 0;

        RetryTimerTask(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers,
            Invoker<T> lastInvoker, int retries, long tick) {
            this.loadbalance = loadbalance;
            this.invocation = invocation;
            this.invokers = invokers;
            this.retries = retries;
            this.tick = tick;
            this.lastInvoker = lastInvoker;
        }

        @Override
        public void run(Timeout timeout) {
            try {

                // 重新调用select，通过负载均衡获取可用的invoker
                Invoker<T> retryInvoker =
                    select(loadbalance, invocation, invokers, Collections.singletonList(lastInvoker));

                // 重试invoker赋值给lastInvoker，在下次重试时作为已使用的invoker被排除
                lastInvoker = retryInvoker;

                // 发起请求
                retryInvoker.invoke(invocation);
            } catch (Throwable e) {
                logger.error("Failed retry to invoke method " + invocation.getMethodName() + ", waiting again.", e);

                // 达到重试上限
                if ((++retryTimes) >= retries) {
                    logger.error(
                        "Failed retry times exceed threshold (" + retries + "), We have to abandon, invocation->"
                            + invocation);
                }

                // 重新提交任务
                else {
                    rePut(timeout);
                }
            }
        }

        private void rePut(Timeout timeout) {

            // 对象初始化的临界情况，可能传入null
            if (timeout == null) {
                return;
            }

            // 校验定时器状态
            Timer timer = timeout.timer();
            if (timer.isStop() || timeout.isCancelled()) {
                return;
            }

            // 重新提交任务
            timer.newTimeout(timeout.task(), tick, TimeUnit.SECONDS);
        }
    }
}
