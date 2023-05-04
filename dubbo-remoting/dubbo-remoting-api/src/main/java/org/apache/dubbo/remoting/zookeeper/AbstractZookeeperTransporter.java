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
import org.apache.dubbo.common.constants.RemotingConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * {@link ZookeeperTransporter}的抽象实现类，提供以下通用能力：
 * 1、通过{@link #zookeeperClientMap}缓存{@link ZookeeperClient}
 * 2、在创建{@link ZookeeperClient}时，如果某个Zookeeper节点无法连接，切换到备用的Zookeeper地址
 * <p>
 * AbstractZookeeperTransporter is abstract implements of ZookeeperTransporter.
 * <p>
 * If you want to extends this, implements createZookeeperClient.
 */
public abstract class AbstractZookeeperTransporter implements ZookeeperTransporter {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperTransporter.class);

    /**
     * {@link ZookeeperClient}缓存：key为zookeeper地址，value为{@link ZookeeperClient}
     * <p>
     * 此处其实只维护一个{@link ZookeeperClient}，不同地址下的{@link ZookeeperClient}其实都一样，通过
     * {@link #writeToClientMap(List, ZookeeperClient)}方法维护
     * <p>
     * 如果连接上Zookeeper后，Zookeeper发生了宕机事件，如果是集群，则由Zookeeper集群本身和Apache Curator共同完成故障转移，此处无需处理
     */
    private final Map<String, ZookeeperClient> zookeeperClientMap = new ConcurrentHashMap<>();

    /**
     * share connect for registry, metadata, etc..
     * <p>
     * Make sure the connection is connected.
     */
    @Override
    public ZookeeperClient connect(URL url) {
        ZookeeperClient zookeeperClient;

        // 获取所有Zookeeper地址
        // address format: {[username:password@]address}
        List<String> addressList = getURLBackupAddress(url);

        // 从缓存中获取一个可用的zookeeperClient（如果有，且连接正常，则更新缓存），且连接正常，则返回
        // The field define the zookeeper server , including protocol, host, port, username, password
        if ((zookeeperClient = fetchAndUpdateZookeeperClientCache(addressList)) != null
            && zookeeperClient.isConnected()) {
            logger.info("find valid zookeeper client from the cache for address: " + url);
            return zookeeperClient;
        }

        // 如果没有，则加锁再次获取，或创建ZookeeperClient
        // avoid creating too many connections， so add lock
        synchronized (zookeeperClientMap) {

            // 再次从缓存中获取一个可用的ZookeeperClient，如果连接正常，则返回
            if ((zookeeperClient = fetchAndUpdateZookeeperClientCache(addressList)) != null
                && zookeeperClient.isConnected()) {
                logger.info("find valid zookeeper client from the cache for address: " + url);
                return zookeeperClient;
            }

            // 如果还没有一个有用的ZookeeperClient，则需要额外创建一个ZookeeperClient
            zookeeperClient = createZookeeperClient(url);
            logger.info("No valid zookeeper client found from cache, therefore create a new client for url. " + url);

            // 更新缓存
            writeToClientMap(addressList, zookeeperClient);
        }
        return zookeeperClient;
    }

    /**
     * @param url the url that will create zookeeper connection .
     *            The url in AbstractZookeeperTransporter#connect parameter is rewritten by this one.
     *            such as: zookeeper://127.0.0.1:2181/org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter
     */
    protected abstract ZookeeperClient createZookeeperClient(URL url);

    /**
     * get the ZookeeperClient from cache, the ZookeeperClient must be connected.
     * <p>
     * It is not private method for unit test.
     */
    public ZookeeperClient fetchAndUpdateZookeeperClientCache(List<String> addressList) {

        ZookeeperClient zookeeperClient = null;

        // 循环地址集合，获取一个有连接的zookeeperClient
        for (String address : addressList) {
            if ((zookeeperClient = zookeeperClientMap.get(address)) != null && zookeeperClient.isConnected()) {
                break;
            }
        }

        // 如果循环除的zookeeperClient可用，则写入到缓存中
        if (zookeeperClient != null && zookeeperClient.isConnected()) {
            writeToClientMap(addressList, zookeeperClient);
        }

        // 返回获取到的zookeeperClient（可能为空）
        return zookeeperClient;
    }

    /**
     * get all zookeeper urls (such as :zookeeper://127.0.0.1:2181?127.0.0.1:8989,127.0.0.1:9999)
     *
     * @param url such as:zookeeper://127.0.0.1:2181?127.0.0.1:8989,127.0.0.1:9999
     * @return such as 127.0.0.1:2181,127.0.0.1:8989,127.0.0.1:9999
     */
    public List<String> getURLBackupAddress(URL url) {
        List<String> addressList = new ArrayList<String>();
        addressList.add(url.getAddress());
        addressList.addAll(url.getParameter(RemotingConstants.BACKUP_KEY, Collections.EMPTY_LIST));

        String authPrefix = null;
        if (StringUtils.isNotEmpty(url.getUsername())) {
            StringBuilder buf = new StringBuilder();
            buf.append(url.getUsername());
            if (StringUtils.isNotEmpty(url.getPassword())) {
                buf.append(":");
                buf.append(url.getPassword());
            }
            buf.append("@");
            authPrefix = buf.toString();
        }

        if (StringUtils.isNotEmpty(authPrefix)) {
            List<String> authedAddressList = new ArrayList<>(addressList.size());
            for (String addr : addressList) {
                authedAddressList.add(authPrefix + addr);
            }
            return authedAddressList;
        }


        return addressList;
    }

    /**
     * write address-ZookeeperClient relationship to Map
     */
    void writeToClientMap(List<String> addressList, ZookeeperClient zookeeperClient) {
        for (String address : addressList) {
            zookeeperClientMap.put(address, zookeeperClient);
        }
    }

    /**
     * redefine the url for zookeeper. just keep protocol, username, password, host, port, and individual parameter.
     */
    URL toClientURL(URL url) {
        Map<String, String> parameterMap = new HashMap<>();
        // for CuratorZookeeperClient
        if (url.getParameter(TIMEOUT_KEY) != null) {
            parameterMap.put(TIMEOUT_KEY, url.getParameter(TIMEOUT_KEY));
        }
        if (url.getParameter(RemotingConstants.BACKUP_KEY) != null) {
            parameterMap.put(RemotingConstants.BACKUP_KEY, url.getParameter(RemotingConstants.BACKUP_KEY));
        }

        return new URL(url.getProtocol(), url.getUsername(), url.getPassword(), url.getHost(), url.getPort(),
                ZookeeperTransporter.class.getName(), parameterMap);
    }

    /**
     * for unit test
     */
    public Map<String, ZookeeperClient> getZookeeperClientMap() {
        return zookeeperClientMap;
    }
}
