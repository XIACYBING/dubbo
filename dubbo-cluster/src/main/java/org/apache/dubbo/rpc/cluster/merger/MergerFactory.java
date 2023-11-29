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

package org.apache.dubbo.rpc.cluster.merger;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.cluster.Merger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 合并工厂类，基于{@link Merger}对多个provider的响应结果进行合并
 *
 * @see org.apache.dubbo.rpc.cluster.support.MergeableClusterInvoker
 */
public class MergerFactory {

    /**
     * 合并器缓存：<要合并的数据类型, 合并器>
     */
    private static final ConcurrentMap<Class<?>, Merger<?>> MERGER_CACHE = new ConcurrentHashMap<Class<?>, Merger<?>>();

    /**
     * Find the merger according to the returnType class, the merger will
     * merge an array of returnType into one
     * <p>
     * 根据类对象{@code returnType}获取对应的合并器，如果是数组则获取数组存储数据的类型的合并器
     *
     * @param returnType the merger will return this type
     * @return the merger which merges an array of returnType into one, return null if not exist
     * @throws IllegalArgumentException if returnType is null
     */
    public static <T> Merger<T> getMerger(Class<T> returnType) {

        // 不可为空
        if (returnType == null) {
            throw new IllegalArgumentException("returnType is null");
        }

        Merger result;

        // 如果是数组，则优先获取数组上的数据对象的合并器
        if (returnType.isArray()) {

            // 获取数组上数据对象的类型
            Class type = returnType.getComponentType();

            // 获取对应合并器
            result = MERGER_CACHE.get(type);

            // 为空则加载合并器后再次获取
            if (result == null) {
                loadMergers();
                result = MERGER_CACHE.get(type);
            }

            // 如果没有数组数据对象的合并器，且数组数据对象非基本数据类型，则直接返回数组合并器
            if (result == null && !type.isPrimitive()) {
                result = ArrayMerger.INSTANCE;
            }
        }

        // 非数组则直接获取对应的合并器
        else {
            result = MERGER_CACHE.get(returnType);
            if (result == null) {
                loadMergers();
                result = MERGER_CACHE.get(returnType);
            }
        }
        return result;
    }

    /**
     * SPI方式加载合并器
     */
    static void loadMergers() {

        // 获取当前支持的所有合并器SPI名称
        Set<String> names = ExtensionLoader.getExtensionLoader(Merger.class).getSupportedExtensions();

        // 循环名称集合，加载对应合并器，并放入集合中
        for (String name : names) {
            Merger m = ExtensionLoader.getExtensionLoader(Merger.class).getExtension(name);

            // 获取具体泛型类对象/正常数据类型作为key
            MERGER_CACHE.putIfAbsent(ReflectUtils.getGenericClass(m.getClass()), m);
        }
    }

}
