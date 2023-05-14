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
package org.apache.dubbo.remoting.exchange.support;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.header.HeaderExchangeHandler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * DefaultFuture.
 * <p>
 * {@link DefaultFuture}其实不会承载任务，本身只是起到数据交流的作用，接收到响应后会将响应结果设置到对应的{@link DefaultFuture}中
 */
public class DefaultFuture extends CompletableFuture<Object> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFuture.class);

    /**
     * 静态常量字段
     * <p>
     * 管理请求{@link Request}和{@link Channel}之间的关系，{@link #CHANNELS}中，key为代表请求的id，value为发送请求的Channel
     */
    private static final Map<Long, Channel> CHANNELS = new ConcurrentHashMap<>();

    /**
     * 静态常量字段
     * <p>
     * 通过id关联DefaultFuture，消费者会先创建DefaultFuture，然后发送请求数据给提供者，{@link HeaderExchangeHandler}接收到响应数据时，会根据
     * {@link Response#getId()}获取到{@link DefaultFuture}，调用{@link DefaultFuture#doReceived(Response)}，将响应数据设置进去
     * <p>
     * id通过{@link org.apache.dubbo.rpc.support.RpcUtils#attachInvocationIdIfAsync}生成
     */
    private static final Map<Long, DefaultFuture> FUTURES = new ConcurrentHashMap<>();

    /**
     * 静态常量字段
     * <p>
     * 检查请求是否超时的时间轮
     */
    public static final Timer TIME_OUT_TIMER =
        new HashedWheelTimer(new NamedThreadFactory("dubbo-future-timeout", true), 30, TimeUnit.MILLISECONDS);

    /**
     * 代表请求的id
     */
    private final Long id;

    /**
     * 发送请求的Channel
     */
    private final Channel channel;

    /**
     * 请求本身
     */
    private final Request request;

    /**
     * 请求超时时间，单位为毫秒，通过{@link #timeoutCheck}提交请求超时检查任务
     */
    private final int timeout;

    /**
     * 提交请求的时间，用来检查请求是否超时
     */
    private final long start = System.currentTimeMillis();

    /**
     * 请求发送完成的时间，基于当前时间判断请求是否发送成功，从而进行一些判断：比如超时是客户端超时还是服务端超时
     */
    private volatile long sent;

    /**
     * 将{@link TimeoutCheckTask}包装成{@link HashedWheelTimer.HashedWheelTimeout}，
     * 然后在到达{@link HashedWheelTimer.HashedWheelTimeout#deadline}后触发{@link HashedWheelTimer.HashedWheelTimeout#task}，
     * 也就是{@link TimeoutCheckTask}
     */
    private Timeout timeoutCheckTask;

    /**
     * 线程池服务，可用来处理响应
     * <p>
     * 如果是{@link ThreadlessExecutor}，则只是将响应处理的任务提交到{@link ThreadlessExecutor#queue}中，由请求线程通过调用
     * {@link ThreadlessExecutor#waitAndDrain()}执行响应处理的任务，进而获取响应结果
     */
    private ExecutorService executor;

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    private DefaultFuture(Channel channel, Request request, int timeout) {
        this.channel = channel;
        this.request = request;
        this.id = request.getId();
        this.timeout = timeout > 0 ? timeout : channel.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
        // put into waiting map.
        FUTURES.put(id, this);
        CHANNELS.put(id, channel);
    }

    /**
     * check time out of the future
     */
    private static void timeoutCheck(DefaultFuture future) {

        // 生成超时检查任务
        TimeoutCheckTask task = new TimeoutCheckTask(future.getId());

        // 提交任务到时间轮中
        future.timeoutCheckTask = TIME_OUT_TIMER.newTimeout(task, future.getTimeout(), TimeUnit.MILLISECONDS);
    }

    /**
     * init a DefaultFuture
     * 1.init a DefaultFuture
     * 2.timeout check
     *
     * @param channel channel
     * @param request the request
     * @param timeout timeout
     * @return a new DefaultFuture
     */
    public static DefaultFuture newFuture(Channel channel, Request request, int timeout, ExecutorService executor) {
        final DefaultFuture future = new DefaultFuture(channel, request, timeout);

        // 设置线程池，在接收到响应时，会根据id获取Future，进而获取线程池，然后将响应处理任务提交给线程池处理
        future.setExecutor(executor);

        // 如果是ThreadLess线程，关联future为waitingFuture，以便设置超时/请求结果等相关内容
        // ThreadlessExecutor needs to hold the waiting future in case of circuit return.
        if (executor instanceof ThreadlessExecutor) {
            ((ThreadlessExecutor)executor).setWaitingFuture(future);
        }

        // 提交超时检查任务到时间轮中
        // timeout check
        timeoutCheck(future);
        return future;
    }

    public static DefaultFuture getFuture(long id) {
        return FUTURES.get(id);
    }

    public static boolean hasFuture(Channel channel) {
        return CHANNELS.containsValue(channel);
    }

    /**
     * 发送完成的处理
     * <p>
     * 执行{@link DefaultFuture#doSent()}，一般是记录发送完成时间戳
     */
    public static void sent(Channel channel, Request request) {
        DefaultFuture future = FUTURES.get(request.getId());
        if (future != null) {
            future.doSent();
        }
    }

    /**
     * close a channel when a channel is inactive
     * directly return the unfinished requests.
     *
     * @param channel channel to close
     */
    public static void closeChannel(Channel channel) {
        for (Map.Entry<Long, Channel> entry : CHANNELS.entrySet()) {

            // 获取当前通道的所有请求
            if (channel.equals(entry.getValue())) {
                DefaultFuture future = getFuture(entry.getKey());
                if (future != null && !future.isDone()) {

                    // 对于未结束的请求，创建一个CHANNEL_INACTIVE的Response，并通知业务线程
                    Response disconnectResponse = new Response(future.getId());
                    disconnectResponse.setStatus(Response.CHANNEL_INACTIVE);
                    disconnectResponse.setErrorMessage(
                        "Channel " + channel + " is inactive. Directly return the unFinished request : "
                            + future.getRequest());
                    DefaultFuture.received(channel, disconnectResponse);
                }
            }
        }
    }

    public static void received(Channel channel, Response response) {
        received(channel, response, false);
    }

    public static void received(Channel channel, Response response, boolean timeout) {
        try {

            // 根据response中的id，获取到消费者创建的DefaultFuture
            DefaultFuture future = FUTURES.remove(response.getId());

            // 不为空的处理
            if (future != null) {

                // 超时检查
                Timeout t = future.timeoutCheckTask;
                if (!timeout) {
                    // decrease Time
                    t.cancel();
                }

                // 将结果设置到future中
                future.doReceived(response);
            }

            // future为空，一般是超时场景，future因超时从FUTURES中移除了
            else {
                logger.warn("The timeout response finally returned at "
                        + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()))
                        + ", response status is " + response.getStatus()
                        + (channel == null ? "" : ", channel: " + channel.getLocalAddress()
                        + " -> " + channel.getRemoteAddress()) + ", please check provider side for detailed result.");
            }
        } finally {

            // 移除已响应请求的channel
            CHANNELS.remove(response.getId());
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {

        // 生成异常情况下的响应
        Response errorResult = new Response(id);
        errorResult.setStatus(Response.CLIENT_ERROR);
        errorResult.setErrorMessage("request future has been canceled.");

        // 通过future处理掉响应
        this.doReceived(errorResult);

        // 从FUTURES和CHANNELS集合中通过id移除当前future
        FUTURES.remove(id);
        CHANNELS.remove(id);
        return true;
    }

    public void cancel() {

        // 调用重写接口，可以中断
        this.cancel(true);
    }

    private void doReceived(Response res) {

        // 响应不能为空
        if (res == null) {
            throw new IllegalStateException("response cannot be null");
        }

        // 状态为OK，调用complete接口设置结果：是在响应确实还没完成的情况下才会设置，如果响应已经完成了，设置会失败
        if (res.getStatus() == Response.OK) {

            // 设置Response中的Result到DefaultFuture的结果中
            // result类型是DecodeableRpcResult，是AppResponse的子类
            this.complete(res.getResult());
        }

        // 消费端或提供端超时，设置异常
        else if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            this.completeExceptionally(new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage()));
        }

        // 其他情况，直接设置异常
        else {
            this.completeExceptionally(new RemotingException(channel, res.getErrorMessage()));
        }

        // 当前处理是一个兜底处理，避免业务线程一直阻塞在ThreadlessExecutor上
        // 到当前链路，代表结果已经被设置，但是ThreadlessExecutor还在等待中（线程池中的任务没有被执行），在任务集合中添加一条任务，调用waitingFuture的get或其他相关方法时，会抛出异常
        // the result is returning, but the caller thread may still waiting
        // to avoid endless waiting for whatever reason, notify caller thread to return.
        if (executor != null && executor instanceof ThreadlessExecutor) {
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor)executor;

            // 线程池还在等待
            if (threadlessExecutor.isWaiting()) {
                threadlessExecutor.notifyReturn(new IllegalStateException(
                    "The result has returned, but the biz thread is still waiting"
                        + " which is not an expected state, interrupt the thread manually by returning an exception."));
            }
        }
    }

    private long getId() {
        return id;
    }

    private Channel getChannel() {
        return channel;
    }

    private boolean isSent() {
        return sent > 0;
    }

    public Request getRequest() {
        return request;
    }

    private int getTimeout() {
        return timeout;
    }

    private void doSent() {
        // 记录发送完成的时间戳
        sent = System.currentTimeMillis();
    }

    private String getTimeoutMessage(boolean scan) {
        long nowTimestamp = System.currentTimeMillis();
        return (sent > 0 ? "Waiting server-side response timeout" : "Sending request timeout in client-side")
                + (scan ? " by scan timer" : "") + ". start time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(start))) + ", end time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(nowTimestamp))) + ","
                + (sent > 0 ? " client elapsed: " + (sent - start)
                + " ms, server elapsed: " + (nowTimestamp - sent)
                : " elapsed: " + (nowTimestamp - start)) + " ms, timeout: "
                + timeout + " ms, request: " + (logger.isDebugEnabled() ? request : getRequestWithoutData()) + ", channel: " + channel.getLocalAddress()
                + " -> " + channel.getRemoteAddress();
    }

    private Request getRequestWithoutData() {
        Request newRequest = request.copy();
        newRequest.setData(null);
        return newRequest;
    }

    /**
     * 超时检查任务
     */
    private static class TimeoutCheckTask implements TimerTask {

        /**
         * 一般来自{@link DefaultFuture#getId()}
         */
        private final Long requestID;

        TimeoutCheckTask(Long requestID) {
            this.requestID = requestID;
        }

        @Override
        public void run(Timeout timeout) {

            /// 获取future
            DefaultFuture future = DefaultFuture.getFuture(requestID);

            // 如果为空（已经因超时或完成被移除），或已完成，则无需处理
            if (future == null || future.isDone()) {
                return;
            }

            // 执行超时检查逻辑（如果future的executor不为空，则提交给executor执行，如果是ThreadlessExecutor，则会等到最后获取结果时一起执行）
            if (future.getExecutor() != null) {
                future.getExecutor().execute(() -> notifyTimeout(future));
            } else {
                notifyTimeout(future);
            }
        }

        private void notifyTimeout(DefaultFuture future) {
            // create exception response.
            Response timeoutResponse = new Response(future.getId());

            // 如果请求已经发送，则任务时服务端超时，否则认为时客户端超时
            // set timeout status.
            timeoutResponse.setStatus(future.isSent() ? Response.SERVER_TIMEOUT : Response.CLIENT_TIMEOUT);

            // 因TimeoutCheckTask任务执行而检查出的超时，所以入参传true，生成对应的提示信息
            timeoutResponse.setErrorMessage(future.getTimeoutMessage(true));

            // 将timeoutResponse设置为future的response
            // handle response.
            DefaultFuture.received(future.getChannel(), timeoutResponse, true);
        }
    }
}
