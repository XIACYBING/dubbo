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

package org.apache.dubbo.registry.retry;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.Constants;
import org.apache.dubbo.registry.support.FailbackRegistry;

import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_PERIOD;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_TIMES;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_PERIOD_KEY;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_TIMES_KEY;

/**
 * 重试任务抽象类，继承{@link TimerTask}，并向时间轮{@link HashedWheelTimer}提交任务
 * <p>
 * 留出{@link #doRetry(URL, FailbackRegistry, Timeout)}给子类实现，是模板方法模式的应用
 * <p>
 * AbstractRetryTask
 */
public abstract class AbstractRetryTask implements TimerTask {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 失败重试url
     * <p>
     * url for retry task
     */
    protected final URL url;

    /**
     * 失败重试注册中心
     * <p>
     * registry for this task
     */
    protected final FailbackRegistry registry;

    /**
     * 重试周期：如果重试失败，下一次重试任务开始的时间，默认值为5000毫秒/5秒{@link Constants#DEFAULT_REGISTRY_RETRY_PERIOD}
     * 单位为毫秒
     * <p>
     * retry period
     */
    final long retryPeriod;

    /**
     * 重试次数，默认值为3{@link Constants#DEFAULT_REGISTRY_RETRY_TIMES}
     * <p>
     * define the most retry times
     */
    private final int retryTimes;

    /**
     * 任务名称
     * <p>
     * task name for this task
     */
    private final String taskName;

    /**
     * 重试次数
     * <p>
     * times of retry.
     * retry task is execute in single thread so that the times is not need volatile.
     */
    private int times = 1;

    private volatile boolean cancel;

    AbstractRetryTask(URL url, FailbackRegistry registry, String taskName) {
        if (url == null || StringUtils.isBlank(taskName)) {
            throw new IllegalArgumentException();
        }
        this.url = url;
        this.registry = registry;
        this.taskName = taskName;
        cancel = false;

        // 获取重试周期毫秒数和最大重试次数
        this.retryPeriod = url.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD);
        this.retryTimes = url.getParameter(REGISTRY_RETRY_TIMES_KEY, DEFAULT_REGISTRY_RETRY_TIMES);
    }

    public void cancel() {
        cancel = true;
    }

    public boolean isCancel() {
        return cancel;
    }

    /**
     * 对任务所属时间轮重新添加任务
     *
     * @param timeout 任务节点
     * @param tick    新任务执行延迟毫秒数
     */
    protected void reput(Timeout timeout, long tick) {
        if (timeout == null) {
            throw new IllegalArgumentException();
        }

        // 获取时间轮，并校验时间轮状态
        Timer timer = timeout.timer();
        if (timer.isStop() || timeout.isCancelled() || isCancel()) {
            return;
        }

        // 重试次数加一
        times++;

        // 向时间轮添加任务
        timer.newTimeout(timeout.task(), tick, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(Timeout timeout) throws Exception {

        // 任务节点、时间轮和任务状态校验
        if (timeout.isCancelled() || timeout.timer().isStop() || isCancel()) {
            // other thread cancel this timeout or stop the timer.
            return;
        }

        // 重试次数已经大于最大重试次数，则不处理
        if (times > retryTimes) {
            // reach the most times of retry.
            logger.warn(
                "Final failed to execute task " + taskName + ", url: " + url + ", retry " + retryTimes + " times.");
            return;
        }

        // 打印重试信息
        if (logger.isInfoEnabled()) {
            logger.info(taskName + " : " + url);
        }

        // 执行重试逻辑
        try {
            doRetry(url, registry, timeout);
        } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
            logger.warn(
                "Failed to execute task " + taskName + ", url: " + url + ", waiting for again, cause:" + t.getMessage(),
                t);

            // 重试失败，将当前重试任务再次向时间轮提交，延迟执行时间retryPeriod毫秒
            // reput this task when catch exception.
            reput(timeout, retryPeriod);
        }
    }

    protected abstract void doRetry(URL url, FailbackRegistry registry, Timeout timeout);
}
