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
package org.apache.dubbo.rpc.cluster.router.condition.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangeType;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.router.AbstractRouter;
import org.apache.dubbo.rpc.cluster.router.condition.ConditionRouter;
import org.apache.dubbo.rpc.cluster.router.condition.config.model.ConditionRouterRule;
import org.apache.dubbo.rpc.cluster.router.condition.config.model.ConditionRuleParser;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract router which listens to dynamic configuration
 *
 * <ol>
 *  {@link ServiceRouter}和{@link AppRouter}都继承了{@link ListenableRouter}，但是没有重写任何方法，只有以下两个区别；
 *
 *  <li>设置了{@link #priority}的值，{@link ServiceRouter#SERVICE_ROUTER_DEFAULT_PRIORITY 140}小于
 *  {@link AppRouter#APP_ROUTER_DEFAULT_PRIORITY 150}，因此{@link ServiceRouter}要优先于{@link AppRouter}执行</li>
 *
 *  <li><@code ruleKey} 规则key不一样，{@link ServiceRouter}的规则key是{@code {interface}:[version]:[group]}三部分构成，
 *  而{@link AppRouter}则是以{@link URL}中的{@link CommonConstants#APPLICATION_KEY}的值作为规则key，这让两者区分了要监听的配置内容</li>
 * </ol>
 *
 * @see ServiceRouter
 * @see AppRouter
 */
public abstract class ListenableRouter extends AbstractRouter implements ConfigurationListener {
    public static final String NAME = "LISTENABLE_ROUTER";
    private static final String RULE_SUFFIX = ".condition-router";

    private static final Logger logger = LoggerFactory.getLogger(ListenableRouter.class);

    /**
     * 条件路由规则
     */
    private ConditionRouterRule routerRule;

    /**
     * 条件路由器集合
     */
    private List<ConditionRouter> conditionRouters = Collections.emptyList();

    public ListenableRouter(URL url, String ruleKey) {
        super(url);
        this.force = false;
        this.init(ruleKey);
    }

    @Override
    public synchronized void process(ConfigChangedEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info("Notification of condition rule, change type is: " + event.getChangeType() +
                    ", raw rule is:\n " + event.getContent());
        }

        if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
            routerRule = null;
            conditionRouters = Collections.emptyList();
        } else {
            try {

                // 生成条件路由规则
                routerRule = ConditionRuleParser.parse(event.getContent());

                // 根据条件路由规则生成条件路由器
                generateConditions(routerRule);
            } catch (Exception e) {
                logger.error("Failed to parse the raw condition rule and it will not take effect, please check " +
                        "if the condition rule matches with the template, the raw rule is:\n " + event.getContent(), e);
            }
        }
    }

    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (CollectionUtils.isEmpty(invokers) || conditionRouters.size() == 0) {
            return invokers;
        }

        // We will check enabled status inside each router.
        for (Router router : conditionRouters) {
            invokers = router.route(invokers, url, invocation);
        }

        return invokers;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public boolean isForce() {
        return (routerRule != null && routerRule.isForce());
    }

    private boolean isRuleRuntime() {
        return routerRule != null && routerRule.isValid() && routerRule.isRuntime();
    }

    private void generateConditions(ConditionRouterRule rule) {
        if (rule != null && rule.isValid()) {
            this.conditionRouters = rule
                .getConditions()
                .stream()
                .map(condition -> new ConditionRouter(condition, rule.isForce(), rule.isEnabled()))
                .collect(Collectors.toList());
        }
    }

    /**
     * 初始化路由规则
     */
    private synchronized void init(String ruleKey) {
        if (StringUtils.isEmpty(ruleKey)) {
            return;
        }

        // 生成要监听配置的key
        String routerKey = ruleKey + RULE_SUFFIX;

        // 注册监听器
        ruleRepository.addListener(routerKey, this);

        // 获取当前的配置，不为空的话就手动调用处理方法进行配置处理
        String rule = ruleRepository.getRule(routerKey, DynamicConfiguration.DEFAULT_GROUP);
        if (StringUtils.isNotEmpty(rule)) {
            this.process(new ConfigChangedEvent(routerKey, DynamicConfiguration.DEFAULT_GROUP, rule));
        }
    }
}
