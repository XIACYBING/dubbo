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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;

/**
 * ConsistentHashLoadBalance
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "consistenthash";

    /**
     * Hash nodes name
     */
    public static final String HASH_NODES = "hash.nodes";

    /**
     * Hash arguments name
     */
    public static final String HASH_ARGUMENTS = "hash.arguments";

    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {

        // 获取方法名称
        String methodName = RpcUtils.getMethodName(invocation);

        // 将serviceKey和方法名称拼接，成为一个key：{group}/{interfaceName}:{version}.{methodName}
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;

        // 获取当前invokers集合的hashCode，
        // using the hashcode of list to compute the hash only pay attention to the elements in the list
        int invokersHashCode = invokers.hashCode();

        // 获取对应的selector
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>)selectors.get(key);

        // 如果selector为空，或两者hashCode不一样（说明invoker的提供者发生了变化）
        // todo loadBalance之前会根据路由过滤invokers，如果这里要依赖于invokers的hashCode去进行loadBalance，是否会导致这里的selector经常被更换？
        if (selector == null || selector.identityHashCode != invokersHashCode) {
            selectors.put(key, new ConsistentHashSelector<T>(invokers, methodName, invokersHashCode));
            selector = (ConsistentHashSelector<T>)selectors.get(key);
        }

        // 使用selector获取一个可用的invoker
        return selector.select(invocation);
    }

    /**
     * todo 对于原理算法和其中的一些算法（比如{@link #hash(byte[], int)}和{@link #virtualInvokers}）其实不太理解
     */
    private static final class ConsistentHashSelector<T> {

        private final TreeMap<Long, Invoker<T>> virtualInvokers;

        private final int replicaNumber;

        private final int identityHashCode;

        private final int[] argumentIndex;

        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {

            // 虚拟invoker集合，可以理解为哈希槽
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();

            // invokers的hashCode
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();

            // 获取需要使用的哈希槽数量，默认是160
            this.replicaNumber = url.getMethodParameter(methodName, HASH_NODES, 160);

            // 获取配置的需要参与哈希计算的参数索引，并将其转换为数字数组
            String[] index = COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, HASH_ARGUMENTS, "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }

            // 循环invokers，填充哈希槽
            for (Invoker<T> invoker : invokers) {

                // 获取invoker的ip和端口地址
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {

                    // 根据invoker地址和i，计算出一个16位的MD5字节数组
                    byte[] digest = Bytes.getMD5(address + i);

                    // h = 0 时，取 digest 中下标为 0~3 的 4 个字节进行位运算
                    // h = 1 时，取 digest 中下标为 4~7 的 4 个字节进行位运算
                    // h = 2 和 h = 3时，过程同上
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        public Invoker<T> select(Invocation invocation) {

            // 将需要参与计算的参数拼接成字符串返回
            String key = toKey(invocation.getArguments());

            // 获取参数字符串的MD5
            byte[] digest = Bytes.getMD5(key);

            // 获取一个invoker
            return selectForKey(hash(digest, 0));
        }

        /**
         * 根据需要参与计算的参数索引数组，获取对应参数，组成字符串返回
         */
        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        private Invoker<T> selectForKey(long hash) {

            // 获取到对应哈希槽位的数据
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.ceilingEntry(hash);

            // 如果为空，则获取第一个哈希槽位的数据
            if (entry == null) {
                entry = virtualInvokers.firstEntry();
            }

            // 返回存储在其中的invoker
            return entry.getValue();
        }

        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }
    }

}
