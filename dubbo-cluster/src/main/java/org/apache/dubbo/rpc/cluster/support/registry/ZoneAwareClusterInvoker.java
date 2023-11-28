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
package org.apache.dubbo.rpc.cluster.support.registry;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationClusterComparator;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationClusterInvoker;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationRule;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationStep;
import org.apache.dubbo.rpc.cluster.support.wrapper.MockClusterInvoker;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.RegistryConstants.LOADBALANCE_AMONG_REGISTRIES;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_ZONE;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_ZONE_FORCE;
import static org.apache.dubbo.common.constants.RegistryConstants.ZONE_KEY;
import static org.apache.dubbo.config.RegistryConfig.PREFER_REGISTRY_KEY;

/**
 * When there're more than one registry for subscription.
 * <p>
 * This extension provides a strategy to decide how to distribute traffics among them:
 * 1. registry marked as 'preferred=true' has the highest priority.
 * 2. check the zone the current request belongs, pick the registry that has the same zone first.
 * 3. Evenly balance traffic between all registries based on each registry's weight.
 * 4. Pick anyone that's available.
 */
public class ZoneAwareClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(ZoneAwareClusterInvoker.class);
    
    private static final String PREFER_REGISTRY_WITH_ZONE_KEY = REGISTRY_KEY + "." + ZONE_KEY;

    private final LoadBalance loadBalanceAmongRegistries = ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(LOADBALANCE_AMONG_REGISTRIES);

    public ZoneAwareClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {

        // 首先从invokers中，获取到registry.preferred配置为true的invoker，该配置代表对应invoker最优先
        // 筛选出第一个配置为true的invoker后，即可发起调用
        // First, pick the invoker (XXXClusterInvoker) that comes from the local registry, distinguish by a 'preferred' key.
        for (Invoker<T> invoker : invokers) {
            ClusterInvoker<T> clusterInvoker = (ClusterInvoker<T>)invoker;
            if (clusterInvoker.isAvailable() && clusterInvoker
                .getRegistryUrl()
                .getParameter(PREFER_REGISTRY_KEY, false)) {
                return clusterInvoker.invoke(invocation);
            }
        }

        // 如果没有，则通过请求配置的registry_zone，筛选到对应的invoker，进行调用
        // providers in the registry with the same zone
        String zone = invocation.getAttachment(REGISTRY_ZONE);
        if (StringUtils.isNotEmpty(zone)) {
            for (Invoker<T> invoker : invokers) {
                ClusterInvoker<T> clusterInvoker = (ClusterInvoker<T>)invoker;
                if (clusterInvoker.isAvailable() && zone.equals(
                    clusterInvoker.getRegistryUrl().getParameter(PREFER_REGISTRY_WITH_ZONE_KEY))) {
                    return clusterInvoker.invoke(invocation);
                }
            }

            // 如果指定可registry_zone，但是没筛选到，且配置了registry_zone_force，则抛出异常
            String force = invocation.getAttachment(REGISTRY_ZONE_FORCE);
            if (StringUtils.isNotEmpty(force) && "true".equalsIgnoreCase(force)) {
                throw new IllegalStateException(
                    "No registry instance in zone or no available providers in the registry, zone: " + zone
                        + ", registries: " + invokers
                        .stream()
                        .map(invoker -> ((MockClusterInvoker<T>)invoker).getRegistryUrl().toString())
                        .collect(Collectors.joining(",")));
            }
        }

        // 如果既没有preferred，也没有指定的registry_zone，则正常通过负载均衡获取可用的invoker
        // 注意，此处负载均衡比较的是各注册中心的权重
        // load balance among all registries, with registry weight count in.
        Invoker<T> balancedInvoker = select(loadBalanceAmongRegistries, invocation, invokers, null);

        // 筛选出可用的invoker，进行调用即可
        if (balancedInvoker.isAvailable()) {
            return balancedInvoker.invoke(invocation);
        }

        // 如果负载均衡也没筛选出可用的invoker，那么循环invokers集合，筛选第一个可用的invoker进行调用即可
        // If none of the invokers has a preferred signal or is picked by the loadbalancer, pick the first one available.
        for (Invoker<T> invoker : invokers) {
            ClusterInvoker<T> clusterInvoker = (ClusterInvoker<T>)invoker;
            if (clusterInvoker.isAvailable()) {
                return clusterInvoker.invoke(invocation);
            }
        }

        // 此处仅为兜底，使用第一个invoker进行调用
        //if none available,just pick one
        return invokers.get(0).invoke(invocation);
    }

    @Override
    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        List<Invoker<T>> invokers = super.list(invocation);

        if (null == invokers || invokers.size() < 2) {
            return invokers;
        }

        List<Invoker<T>> interfaceInvokers = new ArrayList<>();
        List<Invoker<T>> serviceInvokers = new ArrayList<>();
        boolean addressChanged = false;
        for (Invoker<T> invoker : invokers) {
            MigrationClusterInvoker migrationClusterInvoker = (MigrationClusterInvoker) invoker;
            if (migrationClusterInvoker.isServiceInvoker()) {
                serviceInvokers.add(invoker);
            } else {
                interfaceInvokers.add(invoker);
            }

            if (migrationClusterInvoker.invokersChanged().compareAndSet(true, false)) {
                addressChanged = true;
            }
        }

        if (serviceInvokers.isEmpty() || interfaceInvokers.isEmpty()) {
            return invokers;
        }

        MigrationRule rule = null;
        for (Invoker<T> invoker : serviceInvokers) {
            MigrationClusterInvoker migrationClusterInvoker = (MigrationClusterInvoker) invoker;

            if (rule == null) {
                rule = migrationClusterInvoker.getMigrationRule();
                continue;
            }

            // inconsistency rule
            if (!rule.equals(migrationClusterInvoker.getMigrationRule())) {
                rule = MigrationRule.queryRule();
                break;
            }
        }

        MigrationStep step = rule.getStep();

        switch (step) {
            case FORCE_INTERFACE:
                clusterRefresh(addressChanged, interfaceInvokers);
                clusterDestroy(addressChanged, serviceInvokers, true);
                if (logger.isDebugEnabled()) {
                    logger.debug("step is FORCE_INTERFACE");
                }
                return interfaceInvokers;

            case APPLICATION_FIRST:
                clusterRefresh(addressChanged, serviceInvokers);
                clusterRefresh(addressChanged, interfaceInvokers);

                boolean serviceAvailable = !serviceInvokers.isEmpty();
                if (serviceAvailable) {
                    if (shouldMigrate(addressChanged, serviceInvokers, interfaceInvokers)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("step is APPLICATION_FIRST shouldMigrate true get serviceInvokers");
                        }
                        return serviceInvokers;
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("step is APPLICATION_FIRST " + (serviceInvokers.isEmpty() ? "serviceInvokers is empty" : "shouldMigrate false") + " get interfaceInvokers");
                }

                return interfaceInvokers;

            case FORCE_APPLICATION:
                clusterRefresh(addressChanged, serviceInvokers);
                clusterDestroy(addressChanged, interfaceInvokers, true);

                if (logger.isDebugEnabled()) {
                    logger.debug("step is FORCE_APPLICATION");
                }

                return serviceInvokers;
        }

        throw new UnsupportedOperationException(rule.getStep().name());
    }


    private boolean shouldMigrate(boolean addressChanged, List<Invoker<T>> serviceInvokers, List<Invoker<T>> interfaceInvokers) {
        Set<MigrationClusterComparator> detectors = ExtensionLoader.getExtensionLoader(MigrationClusterComparator.class).getSupportedExtensionInstances();
        if (detectors != null && !detectors.isEmpty()) {
            return detectors.stream().allMatch(s -> s.shouldMigrate(interfaceInvokers, serviceInvokers));
        }

        // check application level provider available.
        List<Invoker<T>> availableServiceInvokers = serviceInvokers.stream().filter(s -> s.isAvailable()).collect(Collectors.toList());
        return !availableServiceInvokers.isEmpty();
    }

    private void clusterDestroy(boolean addressChanged, List<Invoker<T>> invokers, boolean destroySub) {
        if (addressChanged) {
            invokers.forEach(s -> {
                MigrationClusterInvoker invoker = (MigrationClusterInvoker) s;
                if (invoker.isServiceInvoker()) {
                    invoker.discardServiceDiscoveryInvokerAddress(invoker);
                    if (destroySub) {
                        invoker.destroyServiceDiscoveryInvoker(invoker);
                    }
                } else {
                    invoker.discardInterfaceInvokerAddress(invoker);
                    if (destroySub) {
                        invoker.destroyInterfaceInvoker(invoker);
                    }
                }
            });
        }
    }

    private void clusterRefresh(boolean addressChanged, List<Invoker<T>> invokers) {
        if (addressChanged) {
            invokers.forEach(s -> {
                MigrationClusterInvoker invoker = (MigrationClusterInvoker) s;
                if (invoker.isServiceInvoker()) {
                    invoker.refreshServiceDiscoveryInvoker();
                } else {
                    invoker.refreshInterfaceInvoker();
                }
            });
        }
    }

}