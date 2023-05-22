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
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.REFERENCE_FILTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SERVICE_FILTER_KEY;

/**
 * {@link Protocol}的装饰器，包装了过滤器的相关功能，基于{@link #buildInvokerChain(Invoker, String, String)}将入参的{@link Invoker}包装成一个{@link FilterNode}，并在其中维护过滤器链
 * <p>
 * todo PR 此处的注释有问题，可以修复
 * ListenerProtocol
 */
@Activate(order = 100)
public class ProtocolFilterWrapper implements Protocol {

    private final Protocol protocol;

    public ProtocolFilterWrapper(Protocol protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("protocol == null");
        }
        this.protocol = protocol;
    }

    /**
     * 将{@code invoker}和{@link Filter}构造成{@link FilterNode}，并组装成一条过滤器链路
     * <p>
     * filter1 -> filter2 -> ... -> filterN -> invoker
     */
    private static <T> Invoker<T> buildInvokerChain(final Invoker<T> invoker, String key, String group) {

        // invoker作为过滤器链的最后一个节点，过滤器结束时会调用对应invoker的invoke方法
        Invoker<T> last = invoker;

        // 根据url中的配置信息获取过滤器拓展集合
        List<Filter> filters =
            ExtensionLoader.getExtensionLoader(Filter.class).getActivateExtension(invoker.getUrl(), key, group);

        if (!filters.isEmpty()) {

            // 从尾到头循环，包装过滤器成FilterNode
            for (int i = filters.size() - 1; i >= 0; i--) {

                // 获取当前过滤器
                final Filter filter = filters.get(i);

                // 将invoker、last（上一节点）和filter作为构造器参数传入
                last = new FilterNode<T>(invoker, last, filter);
            }
        }

        return last;
    }

    @Override
    public int getDefaultPort() {
        return protocol.getDefaultPort();
    }

    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {

        // 注册中心不处理过滤器
        if (UrlUtils.isRegistry(invoker.getUrl())) {
            return protocol.export(invoker);
        }

        // 通过buildInvokerChain构造过滤器链路，并将结果传入protocol.export中
        return protocol.export(buildInvokerChain(invoker, SERVICE_FILTER_KEY, CommonConstants.PROVIDER));
    }

    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {

        // 注册中心不处理过滤器
        if (UrlUtils.isRegistry(url)) {
            return protocol.refer(type, url);
        }

        // 通过protocol.refer获取结果的invoker，并通过buildInvokerChain构造成一条过滤器链
        return buildInvokerChain(protocol.refer(type, url), REFERENCE_FILTER_KEY, CommonConstants.CONSUMER);
    }

    @Override
    public void destroy() {
        protocol.destroy();
    }

    @Override
    public List<ProtocolServer> getServers() {
        return protocol.getServers();
    }

}
