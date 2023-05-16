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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;

/**
 * {@link Protocol}的抽象实现类，维护着{@link #export}和{@link #refer}操作生成的结果（具体的{@code export}和{@code refer}操作交由子类实现），
 * 并提供{@link #destroy()}的通用实现
 * <p>
 * abstract ProtocolSupport.
 */
public abstract class AbstractProtocol implements Protocol {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * {@code export}出去的服务集合
     */
    protected final DelegateExporterMap exporterMap = new DelegateExporterMap();

    /**
     * protocol下的服务器缓存
     * <p>
     * 每个IP地址在同一个协议下只会启动一个服务器，服务器监听对应端口的连接请求，并完成连接创建
     * <p>
     * <host:port, ProtocolServer>
     */
    protected final Map<String, ProtocolServer> serverMap = new ConcurrentHashMap<>();

    /**
     * TODO SoftReference
     * <p>
     * Invoker的缓存
     */
    protected final Set<Invoker<?>> invokers = new ConcurrentHashSet<Invoker<?>>();

    protected static String serviceKey(URL url) {
        int port = url.getParameter(Constants.BIND_PORT_KEY, url.getPort());
        return serviceKey(port, url.getPath(), url.getParameter(VERSION_KEY), url.getParameter(GROUP_KEY));
    }

    /**
     * 获取服务key：{serviceGroup/}serviceName{:serviceVersion}:port
     */
    protected static String serviceKey(int port, String serviceName, String serviceVersion, String serviceGroup) {
        return ProtocolUtils.serviceKey(port, serviceName, serviceVersion, serviceGroup);
    }

    @Override
    public List<ProtocolServer> getServers() {
        return Collections.unmodifiableList(new ArrayList<>(serverMap.values()));
    }

    @Override
    public void destroy() {

        // 遍历refer出去的invoker集合，并调用invoker的destroy方法，释放资源
        for (Invoker<?> invoker : invokers) {
            if (invoker != null) {
                // 从集合中移除当前invoker
                invokers.remove(invoker);
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Destroy reference: " + invoker.getUrl());
                    }
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }

        // 遍历export出去的Exporter，调用相应的unexport方法销毁发布出去的服务
        // Exporter内部可能关联exporterMap，在进行unexport的时候会断开和exporterMap的关联
        for (Map.Entry<String, Exporter<?>> item : exporterMap.getExporterMap().entrySet()) {
            if (exporterMap.removeExportMap(item.getKey(), item.getValue())) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Unexport service: " + item.getValue().getInvoker().getUrl());
                    }
                    item.getValue().unexport();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }
    }

    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        return new AsyncToSyncInvoker<>(protocolBindingRefer(type, url));
    }

    protected abstract <T> Invoker<T> protocolBindingRefer(Class<T> type, URL url) throws RpcException;

    public Map<String, Exporter<?>> getExporterMap() {
        return exporterMap.getExporterMap();
    }

    public Collection<Exporter<?>> getExporters() {
        return exporterMap.getExporters();
    }
}
