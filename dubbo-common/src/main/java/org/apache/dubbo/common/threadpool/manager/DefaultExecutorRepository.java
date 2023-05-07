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
package org.apache.dubbo.common.threadpool.manager;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.threadpool.support.fixed.FixedThreadPool;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NamedThreadFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.EXECUTOR_SERVICE_COMPONENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREADS_KEY;

/**
 * 默认线程池仓库实现
 * <p>
 * Consider implementing {@code Licycle} to enable executors shutdown when the process stops.
 */
public class DefaultExecutorRepository implements ExecutorRepository {
    private static final Logger logger = LoggerFactory.getLogger(DefaultExecutorRepository.class);

    private int DEFAULT_SCHEDULER_SIZE = Runtime.getRuntime().availableProcessors();

    private final ExecutorService SHARED_EXECUTOR =
        Executors.newCachedThreadPool(new NamedThreadFactory("DubboSharedHandler", true));

    private Ring<ScheduledExecutorService> scheduledExecutors = new Ring<>();

    private ScheduledExecutorService serviceExporterExecutor;

    /**
     * 管理线程池的集合：<线程池实现, <端口, 线程池实例>>
     * 线程池实现：以线程池实现作为具体的key，默认值是{@link CommonConstants#EXECUTOR_SERVICE_COMPONENT_KEY}
     * 端口：线程池关联服务的端口，如果是provider服务关联的端口，其实就是相关protocol协议暴露的端口；如果是consumer服务关联的端口，则共享一个线程池
     * 线程池实例：具体的线程池实例，根据URL上的{@link CommonConstants#THREADPOOL_KEY}参数决定，默认是{@link FixedThreadPool}
     *
     * @see <a href="https://github.com/apache/dubbo/issues/7054">consumer端线程池的实现非常不科学</a>
     */
    private ConcurrentMap<String, ConcurrentMap<Integer, ExecutorService>> data = new ConcurrentHashMap<>();

    public DefaultExecutorRepository() {
        for (int i = 0; i < DEFAULT_SCHEDULER_SIZE; i++) {
            ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Dubbo-framework-scheduler"));
            scheduledExecutors.addItem(scheduler);
        }
        serviceExporterExecutor =
            Executors.newScheduledThreadPool(1, new NamedThreadFactory("Dubbo-exporter-scheduler"));
    }

    /**
     * Get called when the server or client instance initiating.
     */
    public synchronized ExecutorService createExecutorIfAbsent(URL url) {

        // 获取默认的线程池集合
        Map<Integer, ExecutorService> executors =
            data.computeIfAbsent(EXECUTOR_SERVICE_COMPONENT_KEY, k -> new ConcurrentHashMap<>());

        // 如果URL上的side是消费者（即当前服务是作为消费者），则使用Integer.MAX_VALUE作为key，否则获取URL上配置实际的key
        //issue-7054:Consumer's executor is sharing globally, key=Integer.MAX_VALUE. Provider's executor is sharing by protocol.
        Integer portKey =
            CONSUMER_SIDE.equalsIgnoreCase(url.getParameter(SIDE_KEY)) ? Integer.MAX_VALUE : url.getPort();

        // 获取或创建线程池
        ExecutorService executor = executors.computeIfAbsent(portKey, k -> createExecutor(url));

        // 如果线程池已关闭，则创建一个新的放入集合
        // If executor has been shut down, create a new one
        if (executor.isShutdown() || executor.isTerminated()) {
            executors.remove(portKey);
            executor = createExecutor(url);
            executors.put(portKey, executor);
        }

        // 返回获取到的线程池
        return executor;
    }

    public ExecutorService getExecutor(URL url) {
        Map<Integer, ExecutorService> executors = data.get(EXECUTOR_SERVICE_COMPONENT_KEY);
        /*
         * It's guaranteed that this method is called after {@link #createExecutorIfAbsent(URL)}, so data should already
         * have Executor instances generated and stored.
         */
        if (executors == null) {
            logger.warn("No available executors, this is not expected, framework should call createExecutorIfAbsent first " +
                    "before coming to here.");
            return null;
        }
        //issue-7054:Consumer's executor is sharing globally, key=Integer.MAX_VALUE. Provider's executor is sharing by protocol.
        Integer portKey = CONSUMER_SIDE.equalsIgnoreCase(url.getParameter(SIDE_KEY)) ? Integer.MAX_VALUE : url.getPort();
        ExecutorService executor = executors.get(portKey);
        if (executor != null && (executor.isShutdown() || executor.isTerminated())) {
            executors.remove(portKey);
            // Does not re-create a shutdown executor, use SHARED_EXECUTOR for downgrade.
            executor = null;
            logger.info("Executor for " + url + " is shutdown.");
        }
        if (executor == null) {
            return SHARED_EXECUTOR;
        } else {
            return executor;
        }
    }

    @Override
    public void updateThreadpool(URL url, ExecutorService executor) {
        try {
            if (url.hasParameter(THREADS_KEY)
                    && executor instanceof ThreadPoolExecutor && !executor.isShutdown()) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
                int threads = url.getParameter(THREADS_KEY, 0);
                int max = threadPoolExecutor.getMaximumPoolSize();
                int core = threadPoolExecutor.getCorePoolSize();
                if (threads > 0 && (threads != max || threads != core)) {
                    if (threads < core) {
                        threadPoolExecutor.setCorePoolSize(threads);
                        if (core == max) {
                            threadPoolExecutor.setMaximumPoolSize(threads);
                        }
                    } else {
                        threadPoolExecutor.setMaximumPoolSize(threads);
                        if (core == max) {
                            threadPoolExecutor.setCorePoolSize(threads);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    @Override
    public ScheduledExecutorService nextScheduledExecutor() {
        return scheduledExecutors.pollItem();
    }

    @Override
    public ScheduledExecutorService getServiceExporterExecutor() {
        return serviceExporterExecutor;
    }

    @Override
    public ExecutorService getSharedExecutor() {
        return SHARED_EXECUTOR;
    }

    @Override
    public void destroyAll() {
        data.values().forEach(executors -> {
            if (executors != null) {
                executors.values().forEach(executor -> {
                    if (executor != null && !executor.isShutdown()) {
                        ExecutorUtil.shutdownNow(executor, 100);
                    }
                });
            }
        });

        data.clear();
    }

    private ExecutorService createExecutor(URL url) {

        // 获取线程池服务：根据url上的threadpool参数获取，默认是FixedThreadPool
        return (ExecutorService)ExtensionLoader.getExtensionLoader(ThreadPool.class)
                                               .getAdaptiveExtension()
                                               .getExecutor(url);
    }

}
