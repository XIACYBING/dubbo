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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.URLStrParser;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.AbstractRegistry;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_SEPARATOR_ENCODED;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;

/**
 * ZookeeperRegistry
 */
public class ZookeeperRegistry extends FailbackRegistry {

    private static final String DEFAULT_ROOT = "dubbo";

    private final String root;

    /**
     * 如果某个URL订阅的*，也就是所有接口，每个接口节点都会记录在当前集合中，一般在这个集合中的接口节点，都有通过{@link #subscribe}订阅过
     */
    private final Set<String> anyServices = new ConcurrentHashSet<>();

    /**
     * 监听的URL和对应的监听器映射，通过{@link ChildListener}在Zookeeper中监听子节点变更，然后通知关联的{@link ChildListener}
     * （并不一定，也可能是关联通知，比如{@link ChildListener#childChanged} ->
     * {@link AbstractRegistry#notify(URL, NotifyListener, List)} ()} -> {@link NotifyListener#notify(List)}）
     */
    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners =
        new ConcurrentHashMap<>();

    private final ZookeeperClient zkClient;

    public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getParameter(GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(PATH_SEPARATOR)) {
            group = PATH_SEPARATOR + group;
        }
        this.root = group;

        // 创建ZookeeperClient
        zkClient = zookeeperTransporter.connect(url);

        // 添加Dubbo和ZK的连接状态监听器，并对RECONNECTED和NEW_SESSION_CREATED状态做出处理
        zkClient.addStateListener((state) -> {

            // 重连时，需要重新注册订阅关系
            if (state == StateListener.RECONNECTED) {
                logger.warn(
                    "Trying to fetch the latest urls, in case there're provider changes during connection loss.\n"
                        + " Since ephemeral ZNode will not get deleted for a connection lose, "
                        + "there's no need to re-register url of this instance.");
                ZookeeperRegistry.this.fetchLatestAddresses();
            }

            // 新Session创建，则进行recovery重新进行注册和订阅，防止数据丢失
            else if (state == StateListener.NEW_SESSION_CREATED) {
                logger.warn("Trying to re-register urls and re-subscribe listeners of this instance to registry...");
                try {
                    ZookeeperRegistry.this.recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else if (state == StateListener.SESSION_LOST) {
                logger.warn("Url of this instance will be deleted from registry soon. "
                    + "Dubbo client will try to re-register once a new session is created.");
            } else if (state == StateListener.SUSPENDED) {

            } else if (state == StateListener.CONNECTED) {

            }
        });
    }

    @Override
    public boolean isAvailable() {
        return zkClient.isConnected();
    }

    @Override
    public void destroy() {

        // 调用父类，回收父类的资源
        super.destroy();
        try {

            // 关闭zk连接
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doRegister(URL url) {
        try {

            // toUrlPath将URL转换为zookeeper上的路径，并通过DYNAMIC_KEY参数判断需要创建临时节点还是永久节点：true为临时节点
            zkClient.create(toUrlPath(url), url.getParameter(DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnregister(URL url) {
        try {

            // 删除url对应的节点
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        try {

            // 当前URL对所有接口都要订阅，其实就是需要订阅根节点/dubbo节点下的子节点数据变更
            // 比如monitor会发生这种订阅请求，因为它需要感知所有Service节点的变化
            if (ANY_VALUE.equals(url.getServiceInterface())) {

                // 获取根节点/dubbo节点
                String root = toRootPath();

                // 获取监听器映射
                ConcurrentMap<NotifyListener, ChildListener> listeners =
                    zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());

                // 根据入参的NotifyListener获取ChildListener
                ChildListener zkListener = listeners.computeIfAbsent(listener, k -> (parentPath, currentChilds) -> {

                    // 循环发生变动的子节点（一般是接口节点，也就是dubbo节点的下一级）
                    for (String child : currentChilds) {

                        // 反编码子节点
                        child = URL.decode(child);

                        // 如果当前节点不在anyServices集合中，说明对当前系统来说是第一次出现的
                        if (!anyServices.contains(child)) {

                            // 添加到集合中
                            anyServices.add(child);

                            // 订阅节点的变更
                            subscribe(url
                                .setPath(child)
                                .addParameters(INTERFACE_KEY, child, Constants.CHECK_KEY, String.valueOf(false)), k);
                        }
                    }
                });

                // 尝试创建dubbo根节点，确保节点存在
                zkClient.create(root, false);

                // 添加子节点变更监听器
                List<String> services = zkClient.addChildListener(root, zkListener);

                // 如果子节点下已有接口节点，那么需要循环处理这部分节点
                if (CollectionUtils.isNotEmpty(services)) {
                    for (String service : services) {

                        // 反编码节点路径
                        service = URL.decode(service);

                        // 添加到anyServices中
                        anyServices.add(service);

                        // 订阅接口节点的变更
                        subscribe(url
                                .setPath(service)
                                .addParameters(INTERFACE_KEY, service, Constants.CHECK_KEY, String.valueOf(false)),
                            listener);
                    }
                }
            } else {
                CountDownLatch latch = new CountDownLatch(1);
                List<URL> urls = new ArrayList<>();

                // 根据不同category进行订阅：一般是providers、configurators和routers，path是转换完成的zookeeper节点路径
                for (String path : toCategoriesPath(url)) {

                    // 获取节点下，NotifyListener和ChildListener的映射
                    ConcurrentMap<NotifyListener, ChildListener> listeners =
                        zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());

                    // 获取入参NotifyListener监听器对应的子节点监听器
                    ChildListener zkListener =
                        listeners.computeIfAbsent(listener, k -> new RegistryChildListenerImpl(url, k, latch));

                    // 如果子节点监听器zkListener的类型是RegistryChildListenerImpl，则需要额外设置latch（防止之前创建的监听器的latch为空？）
                    if (zkListener instanceof RegistryChildListenerImpl) {
                        ((RegistryChildListenerImpl)zkListener).setLatch(latch);
                    }

                    // 尝试根据path创建永久节点，确保节点存在
                    zkClient.create(path, false);

                    // 针对节点创建监听器，监听category节点下的子节点变更（比如consumer监听对应接口的providers节点下的子节点变更情况，从而更新自己本地的providers数据）
                    List<String> children = zkClient.addChildListener(path, zkListener);

                    // 如果当前子节点不为空，则将子节点路径转化为url，添加到urls集合中，后续调用notify方法处理
                    if (children != null) {
                        urls.addAll(toUrlsWithEmpty(url, path, children));
                    }
                }

                // 调用notify方法，处理可能存在的category节点下的数据内容
                notify(url, listener, urls);

                // 倒计时计数，通知相关的监听器可以开始监听工作了
                // tells the listener to run only after the sync notification of main thread finishes.
                latch.countDown();
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {

        // 获取当前url对应的监听器集合
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);

        // 不为空则需要移除订阅关系
        if (listeners != null) {

            // 获取入参的NotifyListener对应的ChildListener
            ChildListener zkListener = listeners.remove(listener);

            // 不为空则需要移除对应监听，根据URL上的interface和category，去移除监听器关系
            if (zkListener != null) {
                if (ANY_VALUE.equals(url.getServiceInterface())) {
                    String root = toRootPath();
                    zkClient.removeChildListener(root, zkListener);
                } else {
                    for (String path : toCategoriesPath(url)) {
                        zkClient.removeChildListener(path, zkListener);
                    }
                }
            }

            // 如果移除之后，监听器集合为空，zkListeners中移除相关的无用映射
            if(listeners.isEmpty()){
                zkListeners.remove(url);
            }
        }
    }

    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            List<String> providers = new ArrayList<>();
            for (String path : toCategoriesPath(url)) {
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private String toRootDir() {
        if (root.equals(PATH_SEPARATOR)) {
            return root;
        }
        return root + PATH_SEPARATOR;
    }

    private String toRootPath() {
        return root;
    }

    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        if (ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        return toRootDir() + URL.encode(name);
    }

    /**
     * 根据url上的category参数，将当前url关注的category对应的zk节点路径组合出来并返回
     */
    private String[] toCategoriesPath(URL url) {
        String[] categories;

        // 如果url上的category参数是*，说明当前url关注所有category的url
        if (ANY_VALUE.equals(url.getParameter(CATEGORY_KEY))) {
            categories =
                new String[] {PROVIDERS_CATEGORY, CONSUMERS_CATEGORY, ROUTERS_CATEGORY, CONFIGURATORS_CATEGORY};
        }

        // 否则获取category参数上配置的数据
        else {
            categories = url.getParameter(CATEGORY_KEY, new String[] {DEFAULT_CATEGORY});
        }

        // 结合url和categories，生成zk节点路径数组：/dubbo/interface（比如：com.xxx.yyy.AService）/category（providers/consumers/routers/configurators）
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            paths[i] = toServicePath(url) + PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    private String toCategoryPath(URL url) {
        return toServicePath(url) + PATH_SEPARATOR + url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
    }

    /**
     * {@link #toRootDir()} + interface（接口路径：com.xxx.yyy.AService） +
     * category（providers/consumers/routers/configurators） + urlFullString
     *
     * @param url url
     * @return 返回拼接完成的zk节点路径
     */
    private String toUrlPath(URL url) {
        return toCategoryPath(url) + PATH_SEPARATOR + URL.encode(url.toFullString());
    }

    private List<URL> toUrlsWithoutEmpty(URL consumer, List<String> providers) {
        List<URL> urls = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(providers)) {
            for (String provider : providers) {
                if (provider.contains(PROTOCOL_SEPARATOR_ENCODED)) {
                    URL url = URLStrParser.parseEncodedStr(provider);
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }

    private List<URL> toUrlsWithEmpty(URL consumer, String path, List<String> providers) {
        List<URL> urls = toUrlsWithoutEmpty(consumer, providers);
        if (CollectionUtils.isEmpty(urls)) {
            int i = path.lastIndexOf(PATH_SEPARATOR);
            String category = i < 0 ? path : path.substring(i + 1);
            URL empty = URLBuilder.from(consumer)
                    .setProtocol(EMPTY_PROTOCOL)
                    .addParameter(CATEGORY_KEY, category)
                    .build();
            urls.add(empty);
        }
        return urls;
    }

    /**
     * When zookeeper connection recovered from a connection loss, it need to fetch the latest provider list.
     * re-register watcher is only a side effect and is not mandate.
     */
    private void fetchLatestAddresses() {
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Fetching the latest urls of " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    removeFailedSubscribed(url, listener);
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    private class RegistryChildListenerImpl implements ChildListener {

        private URL url;

        private NotifyListener listener;

        private volatile CountDownLatch latch;

        RegistryChildListenerImpl(URL url, NotifyListener listener, CountDownLatch latch) {
            this.url = url;
            this.listener = listener;
            this.latch = latch;
        }

        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void childChanged(String path, List<String> children) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warn("Zookeeper children listener thread was interrupted unexpectedly, may cause race condition with the main thread.");
            }
            ZookeeperRegistry.this.notify(url, listener, toUrlsWithEmpty(url, path, children));
        }
    }

}
