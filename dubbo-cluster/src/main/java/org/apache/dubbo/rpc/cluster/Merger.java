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

import org.apache.dubbo.common.extension.SPI;

/**
 * 调用返回结果合并器，目前只提供一些集合数据类型的合并器，比如{@link org.apache.dubbo.rpc.cluster.merger.ArrayMerger 数组}、
 * {@link org.apache.dubbo.rpc.cluster.merger.MapMerger Map}、{@link org.apache.dubbo.rpc.cluster.merger.SetMerger Set
 * }和{@link org.apache.dubbo.rpc.cluster.merger.ListMerger List}
 * <p>
 * 针对数组类型，有提供八种基础数据类型数组的合并器
 *
 * @param <T> 要合并的数据类型
 * @see org.apache.dubbo.rpc.cluster.merger.MergerFactory
 * @see org.apache.dubbo.rpc.cluster.support.MergeableClusterInvoker
 */
@SPI
public interface Merger<T> {

    T merge(T... items);

}
