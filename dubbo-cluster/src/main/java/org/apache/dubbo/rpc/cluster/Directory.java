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

import org.apache.dubbo.common.Node;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;

import java.util.List;

/**
 * 目录，directory，表示多个{@link Invoker}的集合，是后续的路由规则、负载均衡策略和集群容错的基础
 * <p>
 * Directory. (SPI, Prototype, ThreadSafe)
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Directory_service">Directory Service</a>
 *
 * @see org.apache.dubbo.rpc.cluster.Cluster#join(Directory)
 */
public interface Directory<T> extends Node {

    /**
     * 获取接口类型
     * <p>
     * get service type.
     *
     * @return service type.
     */
    Class<T> getInterface();

    /**
     * 根据{@link Invocation}请求，过滤出符合条件的{@link Invoker}集合
     * <p>
     * list invokers.
     *
     * @return invokers
     */
    List<Invoker<T>> list(Invocation invocation) throws RpcException;

    /**
     * 获取当前{@link Directory}维护的所有{@link Invoker}对象
     */
    List<Invoker<T>> getAllInvokers();

    /**
     * 获取consumer端的url
     */
    URL getConsumerUrl();

    /**
     * 当前{@link Directory}是否销毁
     */
    boolean isDestroyed();

    /**
     * todo 看实现似乎是销毁所有invoker的
     */
    void discordAddresses();

}