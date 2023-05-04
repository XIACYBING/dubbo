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
package org.apache.dubbo.remoting.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

/**
 * {@link ZookeeperClient}的抽象实现，提供以下通用能力：
 * 1、缓存当前{@link ZookeeperClient}实例创建的持久化节点在{@link #persistentExistNodePath}
 * 2、管理当前{@link ZookeeperClient}实例添加的各类监听器
 * 2.1、{@link #stateListeners}
 * 2.2、{@link #childListeners}
 * 2.3、{@link #listeners}
 * 3、管理当前{@link ZookeeperClient}实例的运行状态：{@link #closed}
 * <p>
 * 为什么要使用泛型定义，甚至{@link #childListeners}和{@link #listeners}中也要使用泛型进行绑定：因为底层不同框架实现不同，需要通过映射去解耦
 *
 * @param <TargetDataListener>  目标数据监听器类型，在不同框架中有不同实现
 * @param <TargetChildListener> 目标节点的子节点变更监听器，在不同框架中有不同实现
 */
public abstract class AbstractZookeeperClient<TargetDataListener, TargetChildListener> implements ZookeeperClient {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractZookeeperClient.class);

    protected int DEFAULT_CONNECTION_TIMEOUT_MS = 5 * 1000;
    protected int DEFAULT_SESSION_TIMEOUT_MS = 60 * 1000;

    private final URL url;

    /**
     * Dubbo和Zookeeper集群的连接状态的变更监听器集合
     */
    private final Set<StateListener> stateListeners = new CopyOnWriteArraySet<StateListener>();

    /**
     * 子节点变更监听器集合：key为节点路径，value为子节点监听器和框架的目标子节点实现类的映射
     */
    private final ConcurrentMap<String, ConcurrentMap<ChildListener, TargetChildListener>> childListeners =
        new ConcurrentHashMap<String, ConcurrentMap<ChildListener, TargetChildListener>>();

    /**
     * 数据变更监听器：key为节点路径，value为节点数据变更监听器和框架的节点数据监听器实现类的映射
     */
    private final ConcurrentMap<String, ConcurrentMap<DataListener, TargetDataListener>> listeners =
        new ConcurrentHashMap<String, ConcurrentMap<DataListener, TargetDataListener>>();

    /**
     * 连接是否关闭
     */
    private volatile boolean closed = false;

    /**
     * 当前创建的持久化节点路径集合，在创建持久节点前，会先在当前集合中校验是否存在对应节点路径；在创建完成后，会将持久节点路径添加到当前集合中，这减少了和ZK实例的交互（checkExist）
     */
    private final Set<String> persistentExistNodePath = new ConcurrentHashSet<>();

    public AbstractZookeeperClient(URL url) {
        this.url = url;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public void delete(String path) {
        //never mind if ephemeral
        persistentExistNodePath.remove(path);
        deletePath(path);
    }


    @Override
    public void create(String path, boolean ephemeral) {
        if (!ephemeral) {
            if (persistentExistNodePath.contains(path)) {
                return;
            }
            if (checkExists(path)) {
                persistentExistNodePath.add(path);
                return;
            }
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            createEphemeral(path);
        } else {
            createPersistent(path);
            persistentExistNodePath.add(path);
        }
    }

    @Override
    public void addStateListener(StateListener listener) {
        stateListeners.add(listener);
    }

    @Override
    public void removeStateListener(StateListener listener) {
        stateListeners.remove(listener);
    }

    public Set<StateListener> getSessionListeners() {
        return stateListeners;
    }

    @Override
    public List<String> addChildListener(String path, final ChildListener listener) {

        // 获取节点的子节点变更监听器映射：<子节点变更监听器在Dubbo中的实现, 子节点监听器在对应框架上（Curator/Curator 5）的实现>
        ConcurrentMap<ChildListener, TargetChildListener> listeners =
            childListeners.computeIfAbsent(path, k -> new ConcurrentHashMap<>());

        // 获取子节点监听器在对应框架上（Curator/Curator 5）的实现
        TargetChildListener targetListener =
            listeners.computeIfAbsent(listener, k -> createTargetChildListener(path, k));

        // 使用监听器的具体框架实现去监听子节点变更
        return addTargetChildListener(path, targetListener);
    }

    @Override
    public void addDataListener(String path, DataListener listener) {
        this.addDataListener(path, listener, null);
    }

    @Override
    public void addDataListener(String path, DataListener listener, Executor executor) {

        // 获取节点的数据变更监听器映射：<节点数据变更监听器在Dubbo中的实现, 节点数据变更监听器在对应框架上（Curator/Curator 5）的实现>
        ConcurrentMap<DataListener, TargetDataListener> dataListenerMap =
            listeners.computeIfAbsent(path, k -> new ConcurrentHashMap<>());

        // 获取节点数据变更监听器在对应框架上（Curator/Curator 5）的实现
        TargetDataListener targetListener =
            dataListenerMap.computeIfAbsent(listener, k -> createTargetDataListener(path, k));

        // 使用监听器的具体框架实现去监听节点数据变更
        addTargetDataListener(path, targetListener, executor);
    }

    @Override
    public void removeDataListener(String path, DataListener listener) {
        ConcurrentMap<DataListener, TargetDataListener> dataListenerMap = listeners.get(path);
        if (dataListenerMap != null) {
            TargetDataListener targetListener = dataListenerMap.remove(listener);
            if (targetListener != null) {
                removeTargetDataListener(path, targetListener);
            }
        }
    }

    @Override
    public void removeChildListener(String path, ChildListener listener) {
        ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
        if (listeners != null) {
            TargetChildListener targetListener = listeners.remove(listener);
            if (targetListener != null) {
                removeTargetChildListener(path, targetListener);
            }
        }
    }

    protected void stateChanged(int state) {
        for (StateListener sessionListener : getSessionListeners()) {
            sessionListener.stateChanged(state);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            doClose();
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public void create(String path, String content, boolean ephemeral) {
        if (checkExists(path)) {
            delete(path);
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            createEphemeral(path, content);
        } else {
            createPersistent(path, content);
        }
    }

    @Override
    public String getContent(String path) {
        if (!checkExists(path)) {
            return null;
        }
        return doGetContent(path);
    }

    protected abstract void doClose();

    protected abstract void createPersistent(String path);

    protected abstract void createEphemeral(String path);

    protected abstract void createPersistent(String path, String data);

    protected abstract void createEphemeral(String path, String data);

    @Override
    public abstract boolean checkExists(String path);

    protected abstract TargetChildListener createTargetChildListener(String path, ChildListener listener);

    protected abstract List<String> addTargetChildListener(String path, TargetChildListener listener);

    protected abstract TargetDataListener createTargetDataListener(String path, DataListener listener);

    protected abstract void addTargetDataListener(String path, TargetDataListener listener);

    protected abstract void addTargetDataListener(String path, TargetDataListener listener, Executor executor);

    protected abstract void removeTargetDataListener(String path, TargetDataListener listener);

    protected abstract void removeTargetChildListener(String path, TargetChildListener listener);

    protected abstract String doGetContent(String path);

    /**
     * we invoke the zookeeper client to delete the node
     *
     * @param path the node path
     */
    protected abstract void deletePath(String path);

}
