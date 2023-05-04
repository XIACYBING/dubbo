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

import java.util.List;
import java.util.concurrent.Executor;

public interface ZookeeperClient {

    /**
     * 创建ZNode节点
     *
     * @param path      节点路径
     * @param ephemeral true/临时节点，false/永久节点
     */
    void create(String path, boolean ephemeral);

    /**
     * 删除ZNode节点
     *
     * @param path 节点路径
     */
    void delete(String path);

    /**
     * 获取子节点集合
     *
     * @param path 节点路径
     * @return 返回获取到的子节点集合
     */
    List<String> getChildren(String path);

    /**
     * 添加子节点监听器
     *
     * @param path     父节点路径
     * @param listener 监听器
     * @return 返回添加了监听器的子节点
     */
    List<String> addChildListener(String path, ChildListener listener);

    /**
     * 添加节点数据监听器
     *
     * @param path     节点路径，所有子节点的数据变更也会被监听到/directory. All of child of path will be listened.
     * @param listener 监听器
     */
    void addDataListener(String path, DataListener listener);

    /**
     * 添加节点数据监听器
     *
     * @param path     节点路径，所有子节点的数据变更也会被监听到/directory. All of child of path will be listened.
     * @param listener 监听器
     * @param executor 线程池/another thread
     */
    void addDataListener(String path, DataListener listener, Executor executor);

    /**
     * 移除节点对应的数据监听器
     *
     * @param path     节点路径
     * @param listener 监听器
     */
    void removeDataListener(String path, DataListener listener);

    /**
     * 移除子节点监听器
     *
     * @param path     路径
     * @param listener 子节点监听器
     */
    void removeChildListener(String path, ChildListener listener);

    /**
     * 添加Dubbo和Zookeeper集群的连接状态的变更监听器
     *
     * @param listener 状态变更监听器
     */
    void addStateListener(StateListener listener);

    /**
     * 移除Dubbo和Zookeeper集群的连接状态的变更监听器
     *
     * @param listener 状态变更监听器
     */
    void removeStateListener(StateListener listener);

    /**
     * 判断当前连接是否正常
     *
     * @return true/连接正常，false/连接断开
     */
    boolean isConnected();

    /**
     * 关闭当前{@link ZookeeperClient}示例，以及对应的连接
     */
    void close();

    /**
     * 获取zookeeper地址
     *
     * @return 返回获取到的zookeeper地址
     */
    URL getUrl();

    /**
     * 创建节点，并写入内容
     *
     * @param path      节点路径
     * @param content   节点内容
     * @param ephemeral true/临时节点，false/永久节点
     */
    void create(String path, String content, boolean ephemeral);

    /**
     * 获取节点存储的内容
     *
     * @param path 节点路径
     * @return 返回获取到的节点内容
     */
    String getContent(String path);

    /**
     * 判断节点是否存在
     *
     * @param path 节点路径
     * @return true/节点存在，false/节点不存在
     */
    boolean checkExists(String path);

}
