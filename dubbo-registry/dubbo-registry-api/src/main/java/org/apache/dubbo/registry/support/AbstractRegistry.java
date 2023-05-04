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
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.Constants;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.ACCEPTS_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTRY_FILESAVE_SYNC_KEY;
import static org.apache.dubbo.registry.Constants.REGISTRY__LOCAL_FILE_CACHE_ENABLED;

/**
 * 注册中心抽象类，提供以下方法的通用实现：
 *
 * @see #getUrl()
 * @see #register(URL)
 * @see #unregister(URL)
 * @see #subscribe(URL, NotifyListener)
 * @see #unsubscribe(URL, NotifyListener)
 * @see #lookup(URL)
 * @see #destroy()
 * <p>
 * 并通过本地缓存{@link #properties}和{@link #file}为服务提供容错机制，保证服务的可靠性
 * <p>
 * AbstractRegistry. (SPI, Prototype, ThreadSafe)
 */
public abstract class AbstractRegistry implements Registry {

    // URL address separator, used in file cache, service provider URL separation
    private static final char URL_SEPARATOR = ' ';
    // URL address separated regular expression for parsing the service provider URL list in the file cache
    private static final String URL_SPLIT = "\\s+";
    // Max times to retry to save properties to local cache file
    private static final int MAX_RETRY_TIMES_SAVE_PROPERTIES = 3;
    // Log output
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 本地注册信息缓存：为了缓解注册中心压力，我们会将注册/订阅的相关信息缓存到本地，即缓存到{@link #file}指向的文件，两者的数据是同步的
     * <p>
     * {@link #properties}存储的数据为KV结构，大部分key为当前节点（当前服务）作为consumer的url对应的serviceKey，value为Provider的URL列表（这里的Provider
     * 是指providers、routers和configurators）；但是也有一个特殊的key{@code registies}，对应的value是注册中心URL列表
     * <p>
     * Local disk cache, where the special key value.registries records the list of registry centers, and the others are the list of notified service providers
     */
    private final Properties properties = new Properties();

    /**
     * 一个单线程的固定线程池，如果{@link #properties}内容发生变更，并需要同步到{@link #file}，此时如果{@link #syncSaveFile}为false
     * ，则提交同步任务{@link SaveProperties}到当前线程池，由线程池同步数据到{@link #file}中
     * <p>
     * File cache timing writing
     */
    private final ExecutorService registryCacheExecutor =
        Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveRegistryCache", true));

    /**
     * {@link #properties}变更后，是否使用当前前线程同步更新{@link #file}：true/使用当前线程更新，false/向{@link #registryCacheExecutor}提交任务更新
     * <p>
     * Is it synchronized to save the file
     */
    private boolean syncSaveFile;

    /**
     * {@link #properties}和{@link #file}的数据版本，因为每次更新数据都是全覆盖更新，且可能通过异步线程更新，所以需要进行版本控制，防止旧数据覆盖新数据
     */
    private final AtomicLong lastCacheChanged = new AtomicLong();
    private final AtomicInteger savePropertiesRetryTimes = new AtomicInteger();

    /**
     * 当前节点要注册的URL：provider url、consumer url、configurator url或router url
     */
    private final Set<URL> registered = new ConcurrentHashSet<>();

    /**
     * 订阅URL集合，key是被监听的URL，value是监听器集合
     */
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<>();

    /**
     * 通知集合
     * key：当前节点的某个Consumer的URL（一个节点可以消费多个提供者）；
     * value：分类数据集合
     * key：分类，比如providers、routers和configurators
     * value：对应分类的URL集合，比如providers的URL集合
     */
    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<>();

    /**
     * 注册中心地址的URL，包含创建当前{@link Registry}对象的全部配置信息，是被{@link RegistryFactory}修改后的产物
     */
    private URL registryUrl;

    /**
     * 本地缓存的所在的文件，和{@link #properties}对应，会根据{@link #registryUrl}上的{@link Constants#REGISTRY__LOCAL_FILE_CACHE_ENABLED}参数来决定是否将数据缓存到文件中
     * <p>
     *
     * @see Constants#REGISTRY__LOCAL_FILE_CACHE_ENABLED：是否开启文件缓存，显式配置为false时才不开启文件缓存
     * @see Constants#REGISTRY_FILESAVE_SYNC_KEY：是否在{@link #properties}变更后，同步（同一线程）更新变更到{@link #file}
     * @see CommonConstants#FILE_KEY：文件缓存地址，默认值是{/.dubbo/dubbo-registry-[当前应用名]-[当前Registry所在的IP地址].cache}
     * <p>
     * Local disk cache file
     */
    private File file;

    public AbstractRegistry(URL url) {

        // 设置注册中心URL
        setUrl(url);

        // 只有显式配置file.cache为false时，才不开启文件缓存
        if (url.getParameter(REGISTRY__LOCAL_FILE_CACHE_ENABLED, true)) {

            // 是否同步保存数据到文件中
            // Start file save timer
            syncSaveFile = url.getParameter(REGISTRY_FILESAVE_SYNC_KEY, false);

            // 获取文件缓存位置
            String defaultFilename =
                System.getProperty("user.home") + "/.dubbo/dubbo-registry-" + url.getParameter(APPLICATION_KEY) + "-"
                    + url.getAddress().replaceAll(":", "-") + ".cache";
            String filename = url.getParameter(FILE_KEY, defaultFilename);
            File file = null;

            // 创建文件
            if (ConfigUtils.isNotEmpty(filename)) {
                file = new File(filename);
                if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                    if (!file.getParentFile().mkdirs()) {
                        throw new IllegalArgumentException(
                            "Invalid registry cache file " + file + ", cause: Failed to create directory "
                                + file.getParentFile() + "!");
                    }
                }
            }
            this.file = file;

            // 加载文件数据到properties中，以便异常场景的数据正常恢复
            // When starting the subscription center,
            // we need to read the local cache file for future Registry fault tolerance processing.
            loadProperties();

            // 通知当前注册中心的backupUrl
            notify(url.getBackupUrls());
        }
    }

    protected static List<URL> filterEmpty(URL url, List<URL> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            List<URL> result = new ArrayList<>(1);
            result.add(url.setProtocol(EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    @Override
    public URL getUrl() {
        return registryUrl;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    public Set<URL> getRegistered() {
        return Collections.unmodifiableSet(registered);
    }

    public Map<URL, Set<NotifyListener>> getSubscribed() {
        return Collections.unmodifiableMap(subscribed);
    }

    public Map<URL, Map<String, List<URL>>> getNotified() {
        return Collections.unmodifiableMap(notified);
    }

    public File getCacheFile() {
        return file;
    }

    public Properties getCacheProperties() {
        return properties;
    }

    public AtomicLong getLastCacheChanged() {
        return lastCacheChanged;
    }

    public void doSaveProperties(long version) {
        if (version < lastCacheChanged.get()) {
            return;
        }
        if (file == null) {
            return;
        }
        // Save
        try {
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            try (RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
                 FileChannel channel = raf.getChannel()) {
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    throw new IOException("Can not lock the registry cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");
                }
                // Save
                try {
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    try (FileOutputStream outputFile = new FileOutputStream(file)) {
                        properties.store(outputFile, "Dubbo Registry Cache");
                    }
                } finally {
                    lock.release();
                }
            }
        } catch (Throwable e) {
            savePropertiesRetryTimes.incrementAndGet();
            if (savePropertiesRetryTimes.get() >= MAX_RETRY_TIMES_SAVE_PROPERTIES) {
                logger.warn("Failed to save registry cache file after retrying " + MAX_RETRY_TIMES_SAVE_PROPERTIES + " times, cause: " + e.getMessage(), e);
                savePropertiesRetryTimes.set(0);
                return;
            }
            if (version < lastCacheChanged.get()) {
                savePropertiesRetryTimes.set(0);
                return;
            } else {
                registryCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save registry cache file, will retry, cause: " + e.getMessage(), e);
        }
    }

    /**
     * 加载{@link #file}中的数据到{@link #properties}中
     */
    private void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load registry cache file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load registry cache file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * 获取当前URL缓存的相关URL数据：provider url、configurator url和router url
     */
    public List<URL> getCacheUrls(URL url) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String)entry.getKey();
            String value = (String)entry.getValue();
            if (StringUtils.isNotEmpty(key) && key.equals(url.getServiceKey()) && (Character.isLetter(key.charAt(0))
                || key.charAt(0) == '_') && StringUtils.isNotEmpty(value)) {
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<>();
                for (String u : arr) {
                    urls.add(URL.valueOf(u));
                }

                // 匹配到后即返回
                return urls;
            }
        }
        return null;
    }

    @Override
    public List<URL> lookup(URL url) {
        List<URL> result = new ArrayList<>();
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        if (CollectionUtils.isNotEmptyMap(notifiedUrls)) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        } else {
            final AtomicReference<List<URL>> reference = new AtomicReference<>();
            NotifyListener listener = reference::set;
            subscribe(url, listener); // Subscribe logic guarantees the first notify to return
            List<URL> urls = reference.get();
            if (CollectionUtils.isNotEmpty(urls)) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void register(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Register: " + url);
        }

        // 添加url到已注册的集合中，实际的注册逻辑通过子类复写当前方法 + 调用当前方法来实现
        registered.add(url);
    }

    @Override
    public void unregister(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unregister: " + url);
        }

        // 从已注册集合中移除对应URL，实际的解除注册通过子类复写当前方法 + 调用当前方法来实现
        registered.remove(url);
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subscribe: " + url);
        }

        // 将订阅关系添加到subscribe集合中，实际的订阅通过子类复写当前方法 + 调用当前方法来实现
        Set<NotifyListener> listeners = subscribed.computeIfAbsent(url, n -> new ConcurrentHashSet<>());
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unsubscribe: " + url);
        }

        // 将订阅关系从subscribe集合中移除，实际的解除订阅逻辑通过子类复写当前方法 + 调用当前方法来实现
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }

        // do not forget remove notified
        notified.remove(url);
    }

    /**
     * 恢复：当前节点如果和注册中心因为某些原因断开（网络波动，注册中心宕机等...），需要通过当前方法恢复可能缺失的注册数据和订阅关系
     */
    protected void recover() throws Exception {
        // register
        Set<URL> recoverRegistered = new HashSet<>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }

            // 循环集合，调用register方法，恢复注册数据
            for (URL url : recoverRegistered) {
                register(url);
            }
        }
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }

            // 循环集合，调用subscribe方法，恢复订阅关系
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    subscribe(url, listener);
                }
            }
        }
    }

    protected void notify(List<URL> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            return;
        }

        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL url = entry.getKey();

            // 匹配url：interface/path、category、enable、group、version和classifier匹配
            if (!UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }

            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        logger.error("Failed to notify registry event, urls: " + urls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    /**
     * 通知listener，当前url下，对应category以及相关url集合的变更，并保存到{@link #properties}和{@link #file}缓存中
     * <p>
     * Notify changes from the Provider side.
     *
     * @param url      consumer side url
     * @param listener listener
     * @param urls     provider latest urls
     */
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((CollectionUtils.isEmpty(urls)) && !ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }

        // 将urls按照category参数进行分组：providers、routers和configurators
        // keep every provider's category.
        Map<String, List<URL>> result = new HashMap<>();
        for (URL u : urls) {

            // url匹配：interface参数/path路径 -> category参数 -> enabled参数 -> group、version和classified参数
            if (UrlUtils.isMatch(url, u)) {

                // 提取category参数，放入集合中
                String category = u.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
                List<URL> categoryList = result.computeIfAbsent(category, k -> new ArrayList<>());
                categoryList.add(u);
            }
        }

        // 如果分组结果为空，则不处理
        if (result.size() == 0) {
            return;
        }

        // 获取url在notified集合中对应的url集合，也是按照category分组
        Map<String, List<URL>> categoryNotified = notified.computeIfAbsent(url, u -> new ConcurrentHashMap<>());

        // 循环result分组，将result分组内容放入categoryNotified（即变相修改notified中的数据内容），并通知相关监听器
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();

            // 将当前category（providers、routers和configurators）和对应url放入categoryNotified集合中
            categoryNotified.put(category, categoryList);

            // 调用监听器的notify方法，通知url变更
            listener.notify(categoryList);

            // 更新url在本地缓存中的内容
            // We will update our cache file after each notification.
            // When our Registry has a subscribe failure due to network jitter, we can return at least the existing cache URL.
            saveProperties(url);
        }
    }

    /**
     * 更新url下，对应category和相关的url集合在{@link #properties}和{@link #file}中的缓存内容
     */
    private void saveProperties(URL url) {
        if (file == null) {
            return;
        }

        try {
            StringBuilder buf = new StringBuilder();

            // 获取当前url对应的category及相应的url集合，不为空则拼接到buf中，通过URL_SEPARATOR连接
            Map<String, List<URL>> categoryNotified = notified.get(url);
            if (categoryNotified != null) {
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }

            // 根据serviceKey更新properties中的数据
            properties.setProperty(url.getServiceKey(), buf.toString());

            // 更新版本
            long version = lastCacheChanged.incrementAndGet();

            // 根据syncSaveFile标识，判断是同步还是异步更新file内容
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                registryCacheExecutor.execute(new SaveProperties(version));
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public void destroy() {
        if (logger.isInfoEnabled()) {
            logger.info("Destroy registry:" + getUrl());
        }

        // 取消注册
        Set<URL> destroyRegistered = new HashSet<>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<>(destroyRegistered)) {

                // 如果url上显式配置dynamic为false，则不处理，否则调用unregister方法，移除注册数据
                if (url.getParameter(DYNAMIC_KEY, true)) {
                    try {
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn(
                            "Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: "
                                + t.getMessage(), t);
                    }
                }
            }
        }

        // 删除订阅关系
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn(
                            "Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: "
                                + t.getMessage(), t);
                    }
                }
            }
        }

        // 从注册中心工厂中移除当前注册中心
        AbstractRegistryFactory.removeDestroyedRegistry(this);
    }

    protected boolean acceptable(URL urlToRegistry) {

        // 提取注册中心的accepts参数
        String pattern = registryUrl.getParameter(ACCEPTS_KEY);

        // 如果为空，说明没要求，返回true即可
        if (StringUtils.isEmpty(pattern)) {
            return true;
        }

        // 否则要求入参URL的protocol要符合accepts的模式
        return Arrays
            .stream(COMMA_SPLIT_PATTERN.split(pattern))
            .anyMatch(p -> p.equalsIgnoreCase(urlToRegistry.getProtocol()));
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

}
