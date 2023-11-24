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
package org.apache.dubbo.rpc.cluster.configurator;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.cluster.Configurator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACES;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.COMPATIBLE_CONFIG_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CONFIG_VERSION_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.OVERRIDE_PROVIDERS_KEY;

/**
 * AbstractOverrideConfigurator
 */
public abstract class AbstractConfigurator implements Configurator {

    /**
     * 波浪线
     * <ol>
     *     <li>以波浪线开头的url参数，不可被替换</li>
     *     <li>以波浪线开头的url参数，要求在配置器url上的值（不是{@code *}）和在被配置url上的值一致，否之配置器url无法重写被配置url上的参数</li>
     * </ol>
     */
    private static final String TILDE = "~";

    /**
     * 配置器URL
     */
    private final URL configuratorUrl;

    public AbstractConfigurator(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("configurator url == null");
        }
        this.configuratorUrl = url;
    }

    @Override
    public URL getUrl() {
        return configuratorUrl;
    }

    @Override
    public URL configure(URL url) {

        // 如果配置器关闭 || 配置器host为空 || 要配置的url为空 || 要配置url的host为空，则不进行任何重写，直接返回
        // If override url is not enabled or is invalid, just return.
        if (!configuratorUrl.getParameter(ENABLED_KEY, true) || configuratorUrl.getHost() == null || url == null
            || url.getHost() == null) {
            return url;
        }

        // 2.7.0版本后，有configVersion参数
        // This if branch is created since 2.7.0.
        String apiVersion = configuratorUrl.getParameter(CONFIG_VERSION_KEY);

        // 如果是2.7.0以后版本，
        if (StringUtils.isNotEmpty(apiVersion)) {
            String currentSide = url.getParameter(SIDE_KEY);
            String configuratorSide = configuratorUrl.getParameter(SIDE_KEY);

            // 如果是消费端，则使用localHost进行处理
            if (currentSide.equals(configuratorSide) && CONSUMER.equals(configuratorSide)
                && 0 == configuratorUrl.getPort()) {
                url = configureIfMatch(NetUtils.getLocalHost(), url);
            }

            // 如果是提供段，则使用url上的host进行处理
            else if (currentSide.equals(configuratorSide) && PROVIDER.equals(configuratorSide)
                && url.getPort() == configuratorUrl.getPort()) {
                url = configureIfMatch(url.getHost(), url);
            }
        }

        // 2.7.0以前版本的处理
        // This else branch is deprecated and is left only to keep compatibility with versions before 2.7.0
        else {
            url = configureDeprecated(url);
        }
        return url;
    }

    @Deprecated
    private URL configureDeprecated(URL url) {

        // 配置器url上有指定端口，意味着配置器URL上指定的是一个提供者的地址，而我们想要通过该配置器URL控制一个指定的提供者
        // 而这个控制器URL也只对对应的提供者实例，或持有该提供者的消费者生效
        // If override url has port, means it is a provider address. We want to control a specific provider with this override url,
        // it may take effect on the specific provider instance or on consumers holding this provider instance.
        if (configuratorUrl.getPort() != 0) {

            // 匹配入参url上的端口是否和配置器url一致，如果一致，说明当前配置器url可以应用到入参url上
            if (url.getPort() == configuratorUrl.getPort()) {
                return configureIfMatch(url.getHost(), url);
            }
        } else {
            /*
             *  override url don't have a port, means the ip override url specify is a consumer address or 0.0.0.0.
             *  1.If it is a consumer ip address, the intention is to control a specific consumer instance, it must takes effect at the consumer side, any provider received this override url should ignore.
             *  2.If the ip is 0.0.0.0, this override url can be used on consumer, and also can be used on provider.
             */
            if (url.getParameter(SIDE_KEY, PROVIDER).equals(CONSUMER)) {
                // NetUtils.getLocalHost is the ip address consumer registered to registry.
                return configureIfMatch(NetUtils.getLocalHost(), url);
            } else if (url.getParameter(SIDE_KEY, CONSUMER).equals(PROVIDER)) {
                // take effect on all providers, so address must be 0.0.0.0, otherwise it won't flow to this if branch
                return configureIfMatch(ANYHOST_VALUE, url);
            }
        }
        return url;
    }

    /**
     * 在入参url符合要求的情况下，对其进行配置重写
     */
    private URL configureIfMatch(String host, URL url) {

        // 第一重判断：如果配置器url的host是不指定any-host || 入参的host等于配置器url的host，则进行配置重写
        if (ANYHOST_VALUE.equals(configuratorUrl.getHost()) || host.equals(configuratorUrl.getHost())) {
            // TODO, to support wildcards
            String providers = configuratorUrl.getParameter(OVERRIDE_PROVIDERS_KEY);

            // 第二重判断：配置器中未指定要重写的地址 || 要重写的地址中包含入参url的地址 || 要重写的地址中包含any-host
            if (StringUtils.isEmpty(providers) || providers.contains(url.getAddress()) || providers.contains(
                ANYHOST_VALUE)) {
                String configApplication = configuratorUrl.getParameter(APPLICATION_KEY, configuratorUrl.getUsername());
                String currentApplication = url.getParameter(APPLICATION_KEY, url.getUsername());

                // 第三重判断：和第二重判断一样，但是这里判断的是应用
                if (configApplication == null || ANY_VALUE.equals(configApplication) || configApplication.equals(
                    currentApplication)) {

                    // 提取所有以~（波浪线）开头的参数
                    Set<String> tildeKeys = new HashSet<>();
                    for (Map.Entry<String, String> entry : configuratorUrl.getParameters().entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        String tildeKey = StringUtils.isNotEmpty(key) && key.startsWith(TILDE) ? key : null;

                        // 如果配置器url与入参url中以~开头的参数，或application，或side的值不相同，则不使用该配置器url重写入参url
                        // 配置url上值不是*，如果是*，意味着所有值都可以，那么就无需比较两个值是不是相等
                        if (tildeKey != null || APPLICATION_KEY.equals(key) || SIDE_KEY.equals(key)) {
                            if (value != null && !ANY_VALUE.equals(value) && !value.equals(
                                url.getParameter(tildeKey != null ? key.substring(1) : key))) {
                                return url;
                            }
                        }

                        if (tildeKey != null) {
                            tildeKeys.add(tildeKey);
                        }
                    }

                    // 这些参数将会从配置器URL上移除，可能是一些不可动态修改的参数
                    Set<String> conditionKeys = new HashSet<>();
                    conditionKeys.add(CATEGORY_KEY);
                    conditionKeys.add(Constants.CHECK_KEY);
                    conditionKeys.add(DYNAMIC_KEY);
                    conditionKeys.add(ENABLED_KEY);
                    conditionKeys.add(GROUP_KEY);
                    conditionKeys.add(VERSION_KEY);
                    conditionKeys.add(APPLICATION_KEY);
                    conditionKeys.add(SIDE_KEY);
                    conditionKeys.add(CONFIG_VERSION_KEY);
                    conditionKeys.add(COMPATIBLE_CONFIG_KEY);
                    conditionKeys.add(INTERFACES);
                    conditionKeys.addAll(tildeKeys);

                    return doConfigure(url, configuratorUrl.removeParameters(conditionKeys));
                }
            }
        }
        return url;
    }

    /**
     * 执行配置：将{@code configUrl}上的参数赋予{@code currentUrl}
     *
     * @param currentUrl 要被操作的URL
     * @param configUrl  配置URL
     * @return 返回配置后的URL
     * @see org.apache.dubbo.rpc.cluster.configurator.override.OverrideConfigurator#doConfigure(URL, URL)
     * @see org.apache.dubbo.rpc.cluster.configurator.absent.AbsentConfigurator#doConfigure(URL, URL)
     */
    protected abstract URL doConfigure(URL currentUrl, URL configUrl);

}
