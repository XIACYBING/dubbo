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
package org.apache.dubbo.registry;

import org.apache.dubbo.common.Node;
import org.apache.dubbo.common.URL;

/**
 * 继承{@link Node}和{@link RegistryService}，表示{@link Registry}是一个具有注册中心能力的节点，而{@link #reExportRegister(URL)}和{@link #reExportUnregister(URL)}都是委托给{@link RegistryService}去处理
 * <p>
 * Registry只是实际的注册中心（比如Zookeeper）在客户端（provider/consumer）的一个映射，Registry通过和实际的注册中心进行实时信息同步，从而保证provider/consumer
 * 的注册/订阅消息的一致性
 * <p>
 * 注册中心本身只是provider和consumer感知彼此状态变化的一种便捷途径，provider和consumer彼此实际的通讯交互过程对Registry来说是透明无感的
 * <p>
 * Registry. (SPI, Prototype, ThreadSafe)
 *
 * @see org.apache.dubbo.registry.RegistryFactory#getRegistry(URL)
 * @see org.apache.dubbo.registry.support.AbstractRegistry
 */
public interface Registry extends Node, RegistryService {
    default void reExportRegister(URL url) {
        register(url);
    }

    default void reExportUnregister(URL url) {
        unregister(url);
    }
}