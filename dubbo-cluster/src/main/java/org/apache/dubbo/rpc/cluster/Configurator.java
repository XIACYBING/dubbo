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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.PRIORITY_KEY;

/**
 * Configurator. (SPI, Prototype, ThreadSafe)
 * <p>
 * 动态配置URL，来源于注册中心，可通过服务治理控制台写入，用于重写提供者和消费者URL的某些配置信息
 *
 * @see org.apache.dubbo.rpc.cluster.configurator.override.OverrideConfigurator
 * @see org.apache.dubbo.rpc.cluster.configurator.absent.AbsentConfigurator
 */
public interface Configurator extends Comparable<Configurator> {

    /**
     * Get the configurator url.
     *
     * @return configurator url.
     */
    URL getUrl();

    /**
     * Configure the provider url.
     * <p>
     * 配置/重写提供者/消费者的URL的配置信息
     *
     * @param url - old provider url.
     * @return new provider url.
     * @see org.apache.dubbo.rpc.cluster.configurator.AbstractConfigurator#configure(URL)
     */
    URL configure(URL url);

    /**
     * Convert override urls to map for use when re-refer. Send all rules every time, the urls will be reassembled and
     * calculated
     *
     * 转换{@link URL}为{@link Configurator}
     *
     * URL contract:
     * <ol>
     * <li>override://0.0.0.0/...( or override://ip:port...?anyhost=true)&para1=value1... means global rules
     * (all of the providers take effect)</li>
     * <li>override://ip:port...?anyhost=false Special rules (only for a certain provider)</li>
     * <li>override:// rule is not supported... ,needs to be calculated by registry itself</li>
     * <li>override://0.0.0.0/ without parameters means clearing the override</li>
     * </ol>
     *
     * @param urls URL list to convert
     * @return converted configurator list
     */
    static Optional<List<Configurator>> toConfigurators(List<URL> urls) {

        // 为空直接返回空集合
        if (CollectionUtils.isEmpty(urls)) {
            return Optional.empty();
        }

        // 获取Configurator工厂的适配器
        ConfiguratorFactory configuratorFactory =
            ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getAdaptiveExtension();

        List<Configurator> configurators = new ArrayList<>(urls.size());
        for (URL url : urls) {

            // 如果存在empty协议的URL，清空配置器集合，并中止循环
            if (EMPTY_PROTOCOL.equals(url.getProtocol())) {
                configurators.clear();
                break;
            }

            // 获取当前循环URL的所有参数
            Map<String, String> override = new HashMap<>(url.getParameters());
            //The anyhost parameter of override may be added automatically, it can't change the judgement of changing url
            override.remove(ANYHOST_KEY);

            // 如果URL上除了any-host意外没有其他参数，则放弃当前URL
            if (CollectionUtils.isEmptyMap(override)) {
                continue;
            }

            // 否则通过配置器工厂将URL转换为配置器
            configurators.add(configuratorFactory.getConfigurator(url));
        }

        // 排序
        Collections.sort(configurators);

        // 返回配置器集合，Optional
        return Optional.of(configurators);
    }

    /**
     * Sort by host, then by priority
     * 1. the url with a specific host ip should have higher priority than 0.0.0.0
     * 2. if two url has the same host, compare by priority value；
     */
    @Override
    default int compareTo(Configurator o) {
        if (o == null) {
            return -1;
        }

        int ipCompare = getUrl().getHost().compareTo(o.getUrl().getHost());
        // host is the same, sort by priority
        if (ipCompare == 0) {
            int i = getUrl().getParameter(PRIORITY_KEY, 0);
            int j = o.getUrl().getParameter(PRIORITY_KEY, 0);
            return Integer.compare(i, j);
        } else {
            return ipCompare;
        }
    }
}
