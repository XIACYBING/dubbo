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
package org.apache.dubbo.rpc.cluster.router.tag;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangeType;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.router.AbstractRouter;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRouterRule;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRuleParser;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.TAG_KEY;
import static org.apache.dubbo.rpc.Constants.FORCE_USE_TAG;

/**
 * TagRouter, "application.tag-router"
 *
 * @see <a href="https://dubbo.apache.org/zh/docs/advanced/routing-rule/#%E6%A0%87%E7%AD%BE%E8%B7%AF%E7%94%B1%E8%A7%84%E5%88%99"/>
 */
public class TagRouter extends AbstractRouter implements ConfigurationListener {
    public static final String NAME = "TAG_ROUTER";
    private static final int TAG_ROUTER_DEFAULT_PRIORITY = 100;
    private static final Logger logger = LoggerFactory.getLogger(TagRouter.class);
    private static final String RULE_SUFFIX = ".tag-router";

    /**
     * 动态标签路由配置集合
     *
     * @see #process(ConfigChangedEvent)
     */
    private TagRouterRule tagRouterRule;
    private String application;

    public TagRouter(URL url) {
        super(url);
        this.priority = TAG_ROUTER_DEFAULT_PRIORITY;
    }

    @Override
    public synchronized void process(ConfigChangedEvent event) {
        if (logger.isDebugEnabled()) {
            logger.debug("Notification of tag rule, change type is: " + event.getChangeType() + ", raw rule is:\n " +
                    event.getContent());
        }

        try {

            // 标签配置发生变化，进行变更
            if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
                this.tagRouterRule = null;
            } else {
                this.tagRouterRule = TagRuleParser.parse(event.getContent());
            }
        } catch (Exception e) {
            logger.error("Failed to parse the raw tag router rule and it will not take effect, please check if the " +
                    "rule matches with the template, the raw rule is:\n ", e);
        }
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (CollectionUtils.isEmpty(invokers)) {
            return invokers;
        }

        // since the rule can be changed by config center, we should copy one to use.
        final TagRouterRule tagRouterRuleCopy = tagRouterRule;

        // 如果没有动态标签规则，或规则无效/未生效，则使用静态标签（提供者URL上的标签信息）来过滤
        if (tagRouterRuleCopy == null || !tagRouterRuleCopy.isValid() || !tagRouterRuleCopy.isEnabled()) {
            return filterUsingStaticTag(invokers, url, invocation);
        }

        List<Invoker<T>> result = invokers;

        // 提取消费者配置的tag
        String tag = StringUtils.isEmpty(invocation.getAttachment(TAG_KEY)) ? url.getParameter(TAG_KEY) :
                invocation.getAttachment(TAG_KEY);

        // 根据动态路由规则进行标签过滤
        // if we are requesting for a Provider with a specific tag
        if (StringUtils.isNotEmpty(tag)) {

            // 获取对应标签的动态路由规则
            List<String> addresses = tagRouterRuleCopy.getTagnameToAddresses().get(tag);

            // 如果动态路由规则中，对应标签有提供者地址集合，则按照动态路由规则进行提供者/invoker过滤
            // filter by dynamic tag group first
            if (CollectionUtils.isNotEmpty(addresses)) {

                // 根据动态路由规则上配置的地址，和当前地址进行比较并过滤
                result = filterInvoker(invokers, invoker -> addressMatches(invoker.getUrl(), addresses));

                // 如果过滤结果不为空，或动态路由规则有要求强制使用，则直接返回筛选结果
                // if result is not null OR it's null but force=true, return result directly
                if (CollectionUtils.isNotEmpty(result) || tagRouterRuleCopy.isForce()) {
                    return result;
                }
            }

            // 如果没有，则从当前传入的invokers的url上提取tag进行匹配
            else {

                // 如果动态路由规则上未配置地址相关信息，则只筛选invoker上的标签
                // dynamic tag group doesn't have any item about the requested app OR it's null after filtered by
                // dynamic tag group but force=false. check static tag
                result = filterInvoker(invokers, invoker -> tag.equals(invoker.getUrl().getParameter(TAG_KEY)));
            }

            // 如果筛选结果不为空，或消费者有配置强制使用标签，则直接返回筛选结果
            // If there's no tagged providers that can match the current tagged request. force.tag is set by default
            // to false, which means it will invoke any providers without a tag unless it's explicitly disallowed.
            if (CollectionUtils.isNotEmpty(result) || isForceUseTag(invocation)) {
                return result;
            }

            // 匹配结果为空，且非强制使用标签匹配，则进行降级，筛选没有标签信息的invoker返回
            // FAILOVER: return all Providers without any tags.
            else {

                // 筛选出所有不符合动态路由规则配置地址的invoker，todo 不太确认这是为什么，直接从原invokers中筛选出所有不带tag的invoker返回不行吗？
                List<Invoker<T>> tmp = filterInvoker(invokers,
                    invoker -> addressNotMatches(invoker.getUrl(), tagRouterRuleCopy.getAddresses()));
                return filterInvoker(tmp, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
            }
        }

        // 消费者未配置tag
        else {
            // List<String> addresses = tagRouterRule.filter(providerApp);
            // return all addresses in dynamic tag group.

            // 获取动态理由配置中，所有的提供者IP地址
            List<String> addresses = tagRouterRuleCopy.getAddresses();
            if (CollectionUtils.isNotEmpty(addresses)) {

                // 筛选出所有不在动态路由规则配置地址中的invoker，如果为空，说明当前所有invoker都是配置在动态路由规则中的，没有未配置在动态路由规则中的invoker（无可用的provider），直接返回空集合即可
                result = filterInvoker(invokers, invoker -> addressNotMatches(invoker.getUrl(), addresses));
                // 1. all addresses are in dynamic tag group, return empty list.
                if (CollectionUtils.isEmpty(result)) {
                    return result;
                }
                // 2. if there are some addresses that are not in any dynamic tag group, continue to filter using the
                // static tag group.
            }

            // 当前存在invoker不在动态路由规则配置地址中，筛选这部分的invoker并返回
            // 未携带tag的请求，可以请求到url上有tag，但是没配置在动态路由规则上的invoker
            return filterInvoker(result, invoker -> {
                String localTag = invoker.getUrl().getParameter(TAG_KEY);

                // tag为空，或tag非动态路由规则配置的tag
                return StringUtils.isEmpty(localTag) || !tagRouterRuleCopy.getTagNames().contains(localTag);
            });
        }
    }

    /**
     * If there's no dynamic tag rule being set, use static tag in URL.
     * <p>
     * A typical scenario is a Consumer using version 2.7.x calls Providers using version 2.6.x or lower,
     * the Consumer should always respect the tag in provider URL regardless of whether a dynamic tag rule has been set to it or not.
     * <p>
     * TODO, to guarantee consistent behavior of interoperability between 2.6- and 2.7+, this method should has the same logic with the TagRouter in 2.6.x.
     * <p>
     * 从请求中获取标签，按照提供者URL上的标签key进行过滤
     */
    private <T> List<Invoker<T>> filterUsingStaticTag(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        List<Invoker<T>> result;

        // 获取消费者的tag，从invocation中或url上获取
        // Dynamic param
        String tag = StringUtils.isEmpty(invocation.getAttachment(TAG_KEY)) ? url.getParameter(TAG_KEY) :
                invocation.getAttachment(TAG_KEY);
        // Tag request
        if (!StringUtils.isEmpty(tag)) {
            // 从invoker集合中过滤出有相关tag的invoker
            result = filterInvoker(invokers, invoker -> tag.equals(invoker.getUrl().getParameter(TAG_KEY)));

            // 如果没有相关可用的invoker，且当前不强制要求tag，则降级，使用无tag的invoker
            if (CollectionUtils.isEmpty(result) && !isForceUseTag(invocation)) {
                result = filterInvoker(invokers, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
            }
        } else {

            // 当前未配置tag，直接使用无tag的invoker
            result = filterInvoker(invokers, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
        }
        return result;
    }

    @Override
    public boolean isRuntime() {
        return tagRouterRule != null && tagRouterRule.isRuntime();
    }

    @Override
    public boolean isForce() {
        // FIXME
        return tagRouterRule != null && tagRouterRule.isForce();
    }

    private boolean isForceUseTag(Invocation invocation) {
        return Boolean.parseBoolean(invocation.getAttachment(FORCE_USE_TAG, url.getParameter(FORCE_USE_TAG, "false")));
    }

    private <T> List<Invoker<T>> filterInvoker(List<Invoker<T>> invokers, Predicate<Invoker<T>> predicate) {
        if (invokers.stream().allMatch(predicate)) {
            return invokers;
        }

        return invokers.stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    private boolean addressMatches(URL url, List<String> addresses) {
        return addresses != null && checkAddressMatch(addresses, url.getHost(), url.getPort());
    }

    private boolean addressNotMatches(URL url, List<String> addresses) {
        return addresses == null || !checkAddressMatch(addresses, url.getHost(), url.getPort());
    }

    private boolean checkAddressMatch(List<String> addresses, String host, int port) {
        for (String address : addresses) {
            try {
                if (NetUtils.matchIpExpression(address, host, port)) {
                    return true;
                }
                if ((ANYHOST_VALUE + ":" + port).equals(address)) {
                    return true;
                }
            } catch (Exception e) {
                logger.error("The format of ip address is invalid in tag route. Address :" + address, e);
            }
        }
        return false;
    }

    public void setApplication(String app) {
        this.application = app;
    }

    @Override
    public <T> void notify(List<Invoker<T>> invokers) {
        if (CollectionUtils.isEmpty(invokers)) {
            return;
        }

        Invoker<T> invoker = invokers.get(0);
        URL url = invoker.getUrl();
        String providerApplication = url.getParameter(CommonConstants.REMOTE_APPLICATION_KEY);

        if (StringUtils.isEmpty(providerApplication)) {
            logger.error("TagRouter must getConfig from or subscribe to a specific application, but the application " +
                    "in this TagRouter is not specified.");
            return;
        }

        synchronized (this) {
            if (!providerApplication.equals(application)) {
                if (!StringUtils.isEmpty(application)) {
                    ruleRepository.removeListener(application + RULE_SUFFIX, this);
                }
                String key = providerApplication + RULE_SUFFIX;
                ruleRepository.addListener(key, this);
                application = providerApplication;
                String rawRule = ruleRepository.getRule(key, DynamicConfiguration.DEFAULT_GROUP);
                if (StringUtils.isNotEmpty(rawRule)) {
                    this.process(new ConfigChangedEvent(key, DynamicConfiguration.DEFAULT_GROUP, rawRule));
                }
            }
        }
    }

}
