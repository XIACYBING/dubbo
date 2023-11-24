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
package org.apache.dubbo.rpc.cluster.router.tag.model;

import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.cluster.router.AbstractRouterRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * %YAML1.2
 * ---
 * force: true
 * runtime: false
 * enabled: true
 * priority: 1
 * key: demo-provider
 * tags:
 * - name: tag1
 * addresses: [ip1, ip2]
 * - name: tag2
 * addresses: [ip3, ip4]
 * ...
 */
public class TagRouterRule extends AbstractRouterRule {
    private List<Tag> tags;

    /**
     * <提供者IP地址, 标签名称集合>
     */
    private Map<String, List<String>> addressToTagnames = new HashMap<>();

    /**
     * <标签名称, 提供者IP地址集合>
     */
    private Map<String, List<String>> tagnameToAddresses = new HashMap<>();

    public void init() {
        if (!isValid()) {
            return;
        }

        // 将路由标签按照标签和地址纬度进行分组
        tags.stream().filter(tag -> CollectionUtils.isNotEmpty(tag.getAddresses())).forEach(tag -> {

            // 标签 -> 地址
            tagnameToAddresses.put(tag.getName(), tag.getAddresses());

            // 地址 -> 标签
            tag.getAddresses().forEach(addr -> {
                List<String> tagNames = addressToTagnames.computeIfAbsent(addr, k -> new ArrayList<>());
                tagNames.add(tag.getName());
            });
        });
    }

    public List<String> getAddresses() {
        return tags.stream()
                .filter(tag -> CollectionUtils.isNotEmpty(tag.getAddresses()))
                .flatMap(tag -> tag.getAddresses().stream())
                .collect(Collectors.toList());
    }

    public List<String> getTagNames() {
        return tags.stream().map(Tag::getName).collect(Collectors.toList());
    }

    public Map<String, List<String>> getAddressToTagnames() {
        return addressToTagnames;
    }


    public Map<String, List<String>> getTagnameToAddresses() {
        return tagnameToAddresses;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }
}
