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
package org.apache.dubbo.common.threadpool;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The most important difference between this Executor and other normal Executor is that this one doesn't manage
 * any thread.
 * <p>
 * Tasks submitted to this executor through {@link #execute(Runnable)} will not get scheduled to a specific thread, though normal executors always do the schedule.
 * Those tasks are stored in a blocking queue and will only be executed when a thread calls {@link #waitAndDrain()}, the thread executing the task
 * is exactly the same as the one calling waitAndDrain.
 *
 * 当前线程池和其他线程池的最大区别是不管理任何线程，当前线程池只会把任务保存在{@link #queue}集合中，外部必须手动调用{@link #waitAndDrain()}，{@link #waitAndDrain()}
 * 方法会从{@link #queue}中获取Runnable任务，并执行{@link Runnable#run()}，以完成任务
 *
 * 如果{@link #waiting}表示为{@code false}，则表示提交给当前线程池的任务全部由{@link #sharedExecutor}处理
 */
public class ThreadlessExecutor extends AbstractExecutorService {
    private static final Logger logger = LoggerFactory.getLogger(ThreadlessExecutor.class.getName());

    /**
     * 被提交到当前队列的任务，可能是响应处理任务{@link org.apache.dubbo.remoting.transport.dispatcher.ChannelEventRunnable}，由
     * {@link org.apache.dubbo.remoting.transport.dispatcher.WrappedChannelHandler}的实现类提交给当前线程池，然后请求线程执行任务处理
     */
    @SuppressWarnings("JavadocReference")
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

    private ExecutorService sharedExecutor;

    private CompletableFuture<?> waitingFuture;

    /**
     * 当前线程池的任务是否处理完成，会在{@link #waitAndDrain()}方法中被设置为{@code true}
     */
    private boolean finished = false;

    /**
     * 任务是否等待处理：true/任务被提交到{@link #queue}中，直到{@link #waitAndDrain()}被调用，任务才会被处理；false/任务会直接被{@link #sharedExecutor}处理
     * <p>
     * 当前线程池一般和RPC调用绑定，线程池只会被执行一次，后续再次向当前线程池提交任务时，会交给{@link #sharedExecutor}
     */
    private volatile boolean waiting = true;

    private final Object lock = new Object();

    public ThreadlessExecutor(ExecutorService sharedExecutor) {
        this.sharedExecutor = sharedExecutor;
    }

    public CompletableFuture<?> getWaitingFuture() {
        return waitingFuture;
    }

    public void setWaitingFuture(CompletableFuture<?> waitingFuture) {
        this.waitingFuture = waitingFuture;
    }

    public boolean isWaiting() {
        return waiting;
    }

    /**
     * Waits until there is a task, executes the task and all queued tasks (if there're any). The task is either a normal
     * response or a timeout response.
     */
    public void waitAndDrain() throws InterruptedException {
        /*
         * Usually, {@link #waitAndDrain()} will only get called once. It blocks for the response for the first time,
         * once the response (the task) reached and being executed waitAndDrain will return, the whole request process
         * then finishes. Subsequent calls on {@link #waitAndDrain()} (if there're any) should return immediately.
         *
         * There's no need to worry that {@link #finished} is not thread-safe. Checking and updating of
         * 'finished' only appear in waitAndDrain, since waitAndDrain is binding to one RPC call (one thread), the call
         * of it is totally sequential.
         */

        // 如果任务已完成，直接返回
        // finish为true代表和当前线程池绑定的RPC响应已经处理完成
        if (finished) {
            return;
        }

        Runnable runnable;
        try {

            // 获取一个runnable任务：可能会阻塞，一般是响应处理任务
            runnable = queue.take();
        } catch (InterruptedException e) {

            // 执行发生异常，后续提交的任务交给sharedExecutor处理
            waiting = false;
            throw e;
        }

        // 执行任务
        synchronized (lock) {

            // 当前正在执行任务，后续提交的任务交给sharedExecutor处理
            // 执行的任务一般是响应任务
            waiting = false;
            runnable.run();
        }

        // 循环处理queue中剩余的任务，poll不会阻塞
        runnable = queue.poll();
        while (runnable != null) {
            runnable.run();
            runnable = queue.poll();
        }

        // 处理完成所有任务，标记状态
        // mark the status of ThreadlessExecutor as finished.
        finished = true;
    }

    public long waitAndDrain(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        /*long startInMs = System.currentTimeMillis();
        Runnable runnable = queue.poll(timeout, unit);
        if (runnable == null) {
            throw new TimeoutException();
        }
        runnable.run();
        long elapsedInMs = System.currentTimeMillis() - startInMs;
        long timeLeft = timeout - elapsedInMs;
        if (timeLeft < 0) {
            throw new TimeoutException();
        }
        return timeLeft;*/
        throw new UnsupportedOperationException();
    }

    /**
     * If the calling thread is still waiting for a callback task, add the task into the blocking queue to wait for schedule.
     * Otherwise, submit to shared callback executor directly.
     *
     * @param runnable
     */
    @Override
    public void execute(Runnable runnable) {

        // 包装任务
        runnable = new RunnableWrapper(runnable);

        // 提交任务：根据waiting状态进行不同的处理
        // waiting为true一般代表waitAndDrain()已经被调用过了（即相关的RPC请求已经处理完成）
        synchronized (lock) {
            if (!waiting) {
                sharedExecutor.execute(runnable);
            } else {
                queue.add(runnable);
            }
        }
    }

    /**
     * tells the thread blocking on {@link #waitAndDrain()} to return, despite of the current status, to avoid endless waiting.
     */
    public void notifyReturn(Throwable t) {
        // an empty runnable task.
        execute(() -> {

            // 设置异常，当调用get或其他方法时，会抛出该异常
            waitingFuture.completeExceptionally(t);
        });
    }

    /**
     * The following methods are still not supported
     */

    @Override
    public void shutdown() {
        shutdownNow();
    }

    @Override
    public List<Runnable> shutdownNow() {
        notifyReturn(new IllegalStateException("Consumer is shutting down and this call is going to be stopped without " +
                "receiving any result, usually this is called by a slow provider instance or bad service implementation."));
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    private static class RunnableWrapper implements Runnable {
        private Runnable runnable;

        public RunnableWrapper(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                logger.info(t);
            }
        }
    }
}
