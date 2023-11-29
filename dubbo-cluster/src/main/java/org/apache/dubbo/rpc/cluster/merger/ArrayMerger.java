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

import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.rpc.cluster.Merger;

import java.lang.reflect.Array;

/**
 * 数组合并器，直接将多个数组，合并成一个数组即可，在{@link #merge(Object[]...)}方法内的表现则是将入参的二维数组压成一个一维数组
 */
public class ArrayMerger implements Merger<Object[]> {

    public static final ArrayMerger INSTANCE = new ArrayMerger();

    @Override
    public Object[] merge(Object[]... items) {

        // 入参二维数组长度为空，直接返回空集合
        if (ArrayUtils.isEmpty(items)) {
            return new Object[0];
        }

        // 在入参二维数组中找到第一个不为null的一维数组
        int i = 0;
        while (i < items.length && items[i] == null) {
            i++;
        }

        // 如果所有的都为null，直接返回空集合
        if (i == items.length) {
            return new Object[0];
        }

        // 获取第一个不为null的一维数组上的数据类型，要求二维数组上所有一维数组存储数据的数据类型一致，如果不一样需要抛出异常
        Class<?> type = items[i].getClass().getComponentType();

        // 计算最终的一维数组长度
        int totalLen = 0;

        // 循环入参二维数组，校验数据，计算最终数组长度
        for (; i < items.length; i++) {

            // 不处理null
            if (items[i] == null) {
                continue;
            }

            // 校验数据类型
            Class<?> itemType = items[i].getClass().getComponentType();
            if (itemType != type) {
                throw new IllegalArgumentException("Arguments' types are different");
            }

            // 记录一维数组长度
            totalLen += items[i].length;
        }

        // 长度为0，直接返回空数组
        if (totalLen == 0) {
            return new Object[0];
        }

        // 按照数据类型和长度初始化一个数组
        Object result = Array.newInstance(type, totalLen);

        // 循环入参二维数组，提取数据
        int index = 0;
        for (Object[] array : items) {
            if (array != null) {
                for (int j = 0; j < array.length; j++) {
                    Array.set(result, index++, array[j]);
                }
            }
        }

        // 返回最终结果
        return (Object[])result;
    }
}
