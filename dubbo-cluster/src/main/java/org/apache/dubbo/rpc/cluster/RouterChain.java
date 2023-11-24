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
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;

/**
 * 路由规则链路
 * <p>
 * Router chain
 */
public class RouterChain<T> {

    /**
     * full list of addresses from registry, classified by method name.
     * <p>
     * 当前需要过滤的invoker集合
     */
    private List<Invoker<T>> invokers = Collections.emptyList();

    /**
     * containing all routers, reconstruct every time 'route://' urls change.
     * <p>
     * 当前所有的路由规则，路由规则变化时会同步被修改
     * <p>
     * 包含{@link #builtinRouters}
     *
     * @see #addRouters(List)
     */
    private volatile List<Router> routers = Collections.emptyList();

    /**
     * Fixed router instances: ConfigConditionRouter, TagRouter, e.g., the rule for each instance may change but the
     * instance will never delete or recreate.
     * <p>
     * 内置路由规则，一般在构造器中{@link #RouterChain}中由入参url决定
     */
    private List<Router> builtinRouters = Collections.emptyList();

    public static <T> RouterChain<T> buildChain(URL url) {
        return new RouterChain<>(url);
    }

    private RouterChain(URL url) {

        // 获取路由工厂集合
        List<RouterFactory> extensionFactories =
            ExtensionLoader.getExtensionLoader(RouterFactory.class).getActivateExtension(url, ROUTER_KEY);

        // 根据url从路由工厂中获取路由规则
        List<Router> routers =
            extensionFactories.stream().map(factory -> factory.getRouter(url)).collect(Collectors.toList());

        // 初始化路由规则
        initWithRouters(routers);
    }

    /**
     * the resident routers must being initialized before address notification.
     * FIXME: this method should not be public
     */
    public void initWithRouters(List<Router> builtinRouters) {

        // 内置路由规则
        this.builtinRouters = builtinRouters;

        // 初始化所有路由规则
        this.routers = new ArrayList<>(builtinRouters);

        // 路由规则排序
        this.sort();
    }

    /**
     * 根据外部配置路由规则，更新路由规则，只有{@link #builtinRouters}不变
     * <p>
     * If we use route:// protocol in version before 2.7.0, each URL will generate a Router instance, so we should
     * keep the routers up to date, that is, each time router URLs changes, we should update the routers list, only
     * keep the builtinRouters which are available all the time and the latest notified routers which are generated
     * from URLs.
     *
     * @param routers routers from 'router://' rules in 2.6.x or before.
     */
    public void addRouters(List<Router> routers) {

        // 新建路由规则
        List<Router> newRouters = new ArrayList<>();
        newRouters.addAll(builtinRouters);
        newRouters.addAll(routers);
        // CollectionUtils.sort(newRouters);
        this.routers = newRouters;

        // 排序
        this.sort();
    }

    /**
     * @see Router#getPriority()
     * @see Router#compareTo(Router)
     */
    private void sort() {
        Collections.sort(routers);
    }

    public List<Invoker<T>> route(URL url, Invocation invocation) {
        List<Invoker<T>> finalInvokers = invokers;

        // 循环路由规则，对当前invocation调用筛选可用的invokers
        for (Router router : routers) {
            finalInvokers = router.route(finalInvokers, url, invocation);
        }
        return finalInvokers;
    }

    /**
     * 设置{@link #invokers}
     * <p>
     * Notify router chain of the initial addresses from registry at the first time.
     * Notify whenever addresses in registry change.
     */
    public void setInvokers(List<Invoker<T>> invokers) {
        this.invokers = (invokers == null ? Collections.emptyList() : invokers);

        // 通知每个路由规则，invokers发生了变化
        routers.forEach(router -> router.notify(this.invokers));
    }
}
