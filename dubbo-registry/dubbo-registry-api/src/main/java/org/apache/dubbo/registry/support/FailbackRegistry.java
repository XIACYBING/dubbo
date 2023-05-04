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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.retry.FailedRegisteredTask;
import org.apache.dubbo.registry.retry.FailedSubscribedTask;
import org.apache.dubbo.registry.retry.FailedUnregisteredTask;
import org.apache.dubbo.registry.retry.FailedUnsubscribedTask;
import org.apache.dubbo.remoting.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.registry.Constants.CONSUMER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_PERIOD;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_PERIOD_KEY;

/**
 * 对注册中心提供失败重试能力的抽象类，通过重写{@link #register(URL)}、{@link #unregister(URL)}、{@link #subscribe(URL, NotifyListener)}、
 * {@link #unsubscribe(URL, NotifyListener)}和{@link #notify(List)}方法，结合时间轮{@link #retryTimer}，为注册中心的操作提供失败重试能力
 * <p>
 * 对于真正和注册中心交互的方法，则是留出了相关入口给子类实现（模板方法设计模式）：
 *
 * @see #doRegister(URL)
 * @see #doUnregister(URL)
 * @see #doSubscribe(URL, NotifyListener)
 * @see #doUnsubscribe(URL, NotifyListener)
 * @see #doNotify(URL, NotifyListener, List)
 * <p>
 * FailbackRegistry. (SPI, Prototype, ThreadSafe)
 */
public abstract class FailbackRegistry extends AbstractRegistry {

    /*  retry task map */

    /**
     * 失败的{@link #register(URL)}任务集合
     * <p>
     * key是注册失败的URL，value是对应的重试任务
     */
    private final ConcurrentMap<URL, FailedRegisteredTask> failedRegistered =
        new ConcurrentHashMap<URL, FailedRegisteredTask>();

    /**
     * 失败的{@link #unregister(URL)}任务集合
     * <p>
     * key是取消注册失败的URL，value是对应的重试任务
     */
    private final ConcurrentMap<URL, FailedUnregisteredTask> failedUnregistered =
        new ConcurrentHashMap<URL, FailedUnregisteredTask>();

    /**
     * 失败的{@link #subscribe(URL, NotifyListener)}任务集合
     * <p>
     * key是持有订阅失败的{@link URL} + {@link NotifyListener}的{@link Holder}，value是对应的重试任务
     */
    private final ConcurrentMap<Holder, FailedSubscribedTask> failedSubscribed =
        new ConcurrentHashMap<Holder, FailedSubscribedTask>();

    /**
     * 失败的{@link #unsubscribe(URL, NotifyListener)}任务集合
     * <p>
     * key是持有取消订阅失败的{@link URL} + {@link NotifyListener}的{@link Holder}，value是对应的重试任务
     */
    private final ConcurrentMap<Holder, FailedUnsubscribedTask> failedUnsubscribed =
        new ConcurrentHashMap<Holder, FailedUnsubscribedTask>();

    /**
     * 重试操作的时间间隔，单位为毫秒
     * <p>
     * The time in milliseconds the retryExecutor will wait
     */
    private final int retryPeriod;

    /**
     * 提供失败重试能力的时间轮，提供不限次数的重试
     *
     * @see #failedRegistered
     * @see #failedUnregistered
     * @see #failedSubscribed
     * @see #failedUnsubscribed
     * <p>
     * Timer for failure retry, regular check if there is a request for failure, and if there is, an unlimited retry
     */
    private final HashedWheelTimer retryTimer;

    public FailbackRegistry(URL url) {

        // 调用父类构造器方法，完成本地缓存的初始化（properties和file）
        super(url);

        // 设置重试毫秒间隔，默认值为5000毫秒
        this.retryPeriod = url.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD);

        // 新建时间轮，插槽为128个，时间轮周期为5000毫秒
        // since the retry task will not be very much. 128 ticks is enough.
        retryTimer = new HashedWheelTimer(new NamedThreadFactory("DubboRegistryRetryTimer", true), retryPeriod,
            TimeUnit.MILLISECONDS, 128);
    }

    /**
     * 添加订阅失败重试任务
     */
    private void addFailedRegistered(URL url) {

        // 如果待注册url已经有一个失败重试任务，则不重复提交
        FailedRegisteredTask oldOne = failedRegistered.get(url);
        if (oldOne != null) {
            return;
        }

        // 新建失败重试任务
        FailedRegisteredTask newTask = new FailedRegisteredTask(url, this);
        oldOne = failedRegistered.putIfAbsent(url, newTask);

        // 二次检查，添加成功才将任务提交给时间轮
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 移除并取消注册失败重试任务
     */
    private void removeFailedRegistered(URL url) {
        FailedRegisteredTask f = failedRegistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 移除注册失败重试任务
     */
    public void removeFailedRegisteredTask(URL url) {
        failedRegistered.remove(url);
    }

    /**
     * 添加取消注册失败重试任务
     */
    private void addFailedUnregistered(URL url) {

        // 如果url已有取消注册失败重试任务，则不处理
        FailedUnregisteredTask oldOne = failedUnregistered.get(url);
        if (oldOne != null) {
            return;
        }

        // 新建任务，添加到集合中
        FailedUnregisteredTask newTask = new FailedUnregisteredTask(url, this);
        oldOne = failedUnregistered.putIfAbsent(url, newTask);

        // 二次检查，添加成功才将任务提交给时间轮
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 移除并取消 取消注册 失败重试任务
     */
    private void removeFailedUnregistered(URL url) {
        FailedUnregisteredTask f = failedUnregistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 移除取消注册失败重试任务
     */
    public void removeFailedUnregisteredTask(URL url) {
        failedUnregistered.remove(url);
    }

    /**
     * 添加订阅失败重试任务
     */
    protected void addFailedSubscribed(URL url, NotifyListener listener) {

        // 新建持有url和listener的Holder
        Holder h = new Holder(url, listener);

        // 对应Holder之前有订阅失败重试任务，则不处理
        FailedSubscribedTask oldOne = failedSubscribed.get(h);
        if (oldOne != null) {
            return;
        }

        // 添加订阅失败重试任务到时间轮
        FailedSubscribedTask newTask = new FailedSubscribedTask(url, this, listener);
        oldOne = failedSubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 移除并取消 订阅失败重试任务，和取消订阅失败重试任务
     */
    public void removeFailedSubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedSubscribedTask f = failedSubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }

        // 移除并取消取消订阅失败重试任务
        removeFailedUnsubscribed(url, listener);
    }

    /**
     * 移除订阅失败重试任务
     */
    public void removeFailedSubscribedTask(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        failedSubscribed.remove(h);
    }

    /**
     * 添加取消订阅关系失败重试任务
     */
    private void addFailedUnsubscribed(URL url, NotifyListener listener) {

        // 新建持有url和listener的Holder
        Holder h = new Holder(url, listener);

        // 如果已存在重试任务，则不处理
        FailedUnsubscribedTask oldOne = failedUnsubscribed.get(h);
        if (oldOne != null) {
            return;
        }

        // 添加任务
        FailedUnsubscribedTask newTask = new FailedUnsubscribedTask(url, this, listener);
        oldOne = failedUnsubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 移除并取消 取消订阅失败 重试任务
     */
    private void removeFailedUnsubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedUnsubscribedTask f = failedUnsubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 移除取消订阅失败重试任务
     */
    public void removeFailedUnsubscribedTask(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        failedUnsubscribed.remove(h);
    }

    ConcurrentMap<URL, FailedRegisteredTask> getFailedRegistered() {
        return failedRegistered;
    }

    ConcurrentMap<URL, FailedUnregisteredTask> getFailedUnregistered() {
        return failedUnregistered;
    }

    ConcurrentMap<Holder, FailedSubscribedTask> getFailedSubscribed() {
        return failedSubscribed;
    }

    ConcurrentMap<Holder, FailedUnsubscribedTask> getFailedUnsubscribed() {
        return failedUnsubscribed;
    }


    @Override
    public void register(URL url) {

        // 必须要符合模式的url才能注册
        if (!acceptable(url)) {
            logger.info("URL " + url + " will not be registered to Registry. Registry " + url
                + " does not accept service of this protocol type.");
            return;
        }

        // 调用父类的register方法，加入注册URL的集合
        super.register(url);

        // 移除并取消当前URL可能存在的注册失败/取消注册失败的任务
        removeFailedRegistered(url);
        removeFailedUnregistered(url);

        try {
            // 注册URL，这部分逻辑交由子类实现
            // Sending a registration request to the server side
            doRegister(url);
        } catch (Exception e) {
            Throwable t = e;

            // check为true，会直接抛出异常：registryUrl的check参数为true && 待注册url的check参数为true && 待注册url非consumer协议
            // If the startup detection is opened, the Exception is thrown directly.
            boolean check =
                getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true)
                    && !CONSUMER_PROTOCOL.equals(url.getProtocol());

            // skipFailback：跳过失败重试
            boolean skipFailback = t instanceof SkipFailbackWrapperException;

            // check和skipFailback任一为空，则不进行重试
            if (check || skipFailback) {

                // 获取实际的异常
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                    "Failed to register " + url + " to registry " + getUrl().getAddress() + ", cause: "
                        + t.getMessage(), t);
            } else {
                logger.error("Failed to register " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 添加注册失败任务
            // Record a failed registration request to a failed list, retry regularly
            addFailedRegistered(url);
        }
    }

    @Override
    public void reExportRegister(URL url) {

        // 必须要符合模式的url才能注册
        if (!acceptable(url)) {
            logger.info("URL " + url + " will not be registered to Registry. Registry " + url
                + " does not accept service of this protocol type.");
            return;
        }

        // 调用父类的register方法，加入注册URL的集合
        super.register(url);

        // 移除并取消当前URL可能存在的注册失败/取消注册失败的任务
        removeFailedRegistered(url);
        removeFailedUnregistered(url);

        // 执行注册
        try {
            // 注册URL，这部分逻辑交由子类实现
            // Sending a registration request to the server side
            doRegister(url);
        } catch (Exception e) {
            if (!(e instanceof SkipFailbackWrapperException)) {
                throw new IllegalStateException(
                    "Failed to register (re-export) " + url + " to registry " + getUrl().getAddress() + ", cause: "
                        + e.getMessage(), e);
            }
        }
    }

    @Override
    public void unregister(URL url) {

        // 调用父类方法，将对应URL从注册URL集合中移除
        super.unregister(url);

        // 移除并取消当前URL可能存在的注册失败/取消注册失败的任务
        removeFailedRegistered(url);
        removeFailedUnregistered(url);

        try {
            // 取消注册URL，这部分逻辑交由子类实现
            // Sending a cancellation request to the server side
            doUnregister(url);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true)
                    && !CONSUMER_PROTOCOL.equals(url.getProtocol());
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to unregister " + url + " to registry " + getUrl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to unregister " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 添加取消注册重试任务
            // Record a failed registration request to a failed list, retry regularly
            addFailedUnregistered(url);
        }
    }

    @Override
    public void reExportUnregister(URL url) {

        // 调用父类方法，将对应URL从注册URL集合中移除
        super.unregister(url);

        // 移除并取消当前URL可能存在的注册失败/取消注册失败的任务
        removeFailedRegistered(url);
        removeFailedUnregistered(url);

        try {
            // 取消注册URL，这部分逻辑交由子类实现
            // Sending a cancellation request to the server side
            doUnregister(url);
        } catch (Exception e) {
            if (!(e instanceof SkipFailbackWrapperException)) {
                throw new IllegalStateException(
                    "Failed to unregister(re-export) " + url + " to registry " + getUrl().getAddress() + ", cause: "
                        + e.getMessage(), e);
            }
        }
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {

        // 调用父类方法，将订阅关系加入父类的集合中
        super.subscribe(url, listener);

        // 移除并取消可能存在的订阅失败重试任务（内部也会移除并取消 取消订阅失败重试任务）
        removeFailedSubscribed(url, listener);
        try {

            // 执行订阅，交由子类实现
            // Sending a subscription request to the server side
            doSubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            // 如果当前url在本地缓存中存储的对应的providers、routers和configurators相关的url集合不为空，则通过notify方法通知listener，告知urls的变更情况
            // 并更新本地缓存中url下的相关缓存数据
            List<URL> urls = getCacheUrls(url);
            if (CollectionUtils.isNotEmpty(urls)) {
                notify(url, listener, urls);
                logger.error("Failed to subscribe " + url + ", Using cached list: " + urls + " from cache file: "
                    + getUrl().getParameter(FILE_KEY,
                    System.getProperty("user.home") + "/dubbo-registry-" + url.getHost() + ".cache") + ", cause: "
                    + t.getMessage(), t);
            }

            // 否则校验，判断是否抛出异常
            else {
                // If the startup detection is opened, the Exception is thrown directly.
                boolean check =
                    getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true);
                boolean skipFailback = t instanceof SkipFailbackWrapperException;
                if (check || skipFailback) {
                    if (skipFailback) {
                        t = t.getCause();
                    }
                    throw new IllegalStateException("Failed to subscribe " + url + ", cause: " + t.getMessage(), t);
                } else {
                    logger.error("Failed to subscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
                }
            }

            // Record a failed registration request to a failed list, retry regularly
            addFailedSubscribed(url, listener);
        }
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {

        // 调用父类方法，移除相关的订阅关系
        super.unsubscribe(url, listener);

        // 移除并取消可能存在的取消订阅失败重试任务（内部也会移除并取消 取消订阅失败重试任务）
        removeFailedSubscribed(url, listener);

        try {
            // 执行取消订阅，交由子类实现
            // Sending a canceling subscription request to the server side
            doUnsubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to unsubscribe " + url + " to registry " + getUrl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to unsubscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 添加取消订阅失败重试任务
            // Record a failed registration request to a failed list, retry regularly
            addFailedUnsubscribed(url, listener);
        }
    }

    @Override
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        try {
            doNotify(url, listener, urls);
        } catch (Exception t) {
            // Record a failed registration request to a failed list
            logger.error("Failed to notify addresses for subscribe " + url + ", cause: " + t.getMessage(), t);
        }
    }

    protected void doNotify(URL url, NotifyListener listener, List<URL> urls) {
        super.notify(url, listener, urls);
    }

    @Override
    protected void recover() throws Exception {

        // 获取已注册的url集合
        // register
        Set<URL> recoverRegistered = new HashSet<URL>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }

            // 循环已注册的url集合，移除之前可能存在的注册/取消注册失败重试任务，并添加一个注册失败重试任务
            for (URL url : recoverRegistered) {
                // remove fail registry or unRegistry task first.
                removeFailedRegistered(url);
                removeFailedUnregistered(url);
                addFailedRegistered(url);
            }
        }

        // 获取已存在的订阅关系
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }

            // 循环已存在的订阅关系，移除之前可能存在的订阅关系失败重试任务，并重新添加一个订阅失败重试任务
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    // First remove other tasks to ensure that addFailedSubscribed can succeed.
                    removeFailedSubscribed(url, listener);
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    @Override
    public void destroy() {

        // 销毁：取消注册、取消订阅关系、将当前注册中心从注册中心集合中移除
        super.destroy();

        // 失败重试时间轮停止
        retryTimer.stop();
    }

    // ==== Template method ====

    public abstract void doRegister(URL url);

    public abstract void doUnregister(URL url);

    public abstract void doSubscribe(URL url, NotifyListener listener);

    public abstract void doUnsubscribe(URL url, NotifyListener listener);

    static class Holder {

        private final URL url;

        private final NotifyListener notifyListener;

        Holder(URL url, NotifyListener notifyListener) {
            if (url == null || notifyListener == null) {
                throw new IllegalArgumentException();
            }
            this.url = url;
            this.notifyListener = notifyListener;
        }

        @Override
        public int hashCode() {
            return url.hashCode() + notifyListener.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Holder) {
                Holder h = (Holder) obj;
                return this.url.equals(h.url) && this.notifyListener.equals(h.notifyListener);
            } else {
                return false;
            }
        }
    }
}
