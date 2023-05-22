/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.dubbo.common.threadlocal;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * {@link InternalThreadLocal}和{@link ThreadLocal}最大的区别是，普通的{@link ThreadLocal}是用{@link ThreadLocal}实例作为key和对应的值关联，key的hashcode
 * 全局唯一；{@link InternalThreadLocal}则是用内部存储的{@link InternalThreadLocal#index}作为索引位关联对应的值，那么为了保证正确性，index也要全局唯一才行
 * <p>
 * InternalThreadLocal
 * A special variant of {@link ThreadLocal} that yields higher access performance when accessed from a
 * {@link InternalThread}.
 * <p></p>
 * Internally, a {@link InternalThread} uses a constant index in an array, instead of using hash code and hash table,
 * to look for a variable.  Although seemingly very subtle, it yields slight performance advantage over using a hash
 * table, and it is useful when accessed frequently.
 * <p></p>
 * This design is learning from {@see io.netty.util.concurrent.FastThreadLocal} which is in Netty.
 */
public class InternalThreadLocal<V> {

    /**
     * 待删除的{@link InternalThreadLocal}集合所在的索引位
     * <p>
     * 固定索引，索引对应的位置用来存储对应线程上所有的{@link InternalThreadLocal}实例，以便在线程结束时清理
     * <p>
     * 当前索引位的数据集合只是个兜底操作，如果使用者有自己调用{@link #remove()}接口，那么数据在从线程绑定的{@link InternalThreadLocalMap}中移除的同时，也会从待删除集合中移除
     *
     * @see Set<InternalThreadLocal>
     * @see #removeAll()
     * @see #addToVariablesToRemove(InternalThreadLocalMap, InternalThreadLocal)
     * @see #removeFromVariablesToRemove(InternalThreadLocalMap, InternalThreadLocal)
     */
    private static final int VARIABLES_TO_REMOVE_INDEX = InternalThreadLocalMap.nextVariableIndex();

    /**
     * 当前{@link InternalThreadLocal}的索引位，代表当前{@link InternalThreadLocal}的数据存储在
     * {@link InternalThreadLocalMap#indexedVariables}数组中的哪个位置
     */
    private final int index;

    public InternalThreadLocal() {
        index = InternalThreadLocalMap.nextVariableIndex();
    }

    /**
     * 移除当前线程上关联的所有{@link InternalThreadLocal}，一般在线程结束时被调用
     * <p>
     * Removes all {@link InternalThreadLocal} variables bound to the current thread.  This operation is useful when you
     * are in a container environment, and you don't want to leave the thread local variables in the threads you do not
     * manage.
     */
    @SuppressWarnings("unchecked")
    public static void removeAll() {

        // 获取线程上关联的InternalThreadLocalMap，如果没有则无需处理
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return;
        }

        try {

            // 获取固定位置的数据
            Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);

            // 对应位置有设置数据，说明线程在执行过程中有使用过InternalThreadLocal，需要清除这部分数据
            if (v != null && v != InternalThreadLocalMap.UNSET) {

                // 获取InternalThreadLocal实例集合
                Set<InternalThreadLocal<?>> variablesToRemove = (Set<InternalThreadLocal<?>>)v;
                InternalThreadLocal<?>[] variablesToRemoveArray = variablesToRemove.toArray(new InternalThreadLocal[0]);

                // 循环InternalThreadLocal数据集合，从对应的InternalThreadLocalMap上移除对应的InternalThreadLocal数据
                for (InternalThreadLocal<?> tlv : variablesToRemoveArray) {
                    tlv.remove(threadLocalMap);
                }
            }
        } finally {

            // 移除当前线程上关联的InternalThreadLocalMap
            InternalThreadLocalMap.remove();
        }
    }

    /**
     * Returns the number of thread local variables bound to the current thread.
     */
    public static int size() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return 0;
        } else {
            return threadLocalMap.size();
        }
    }

    public static void destroy() {
        InternalThreadLocalMap.destroy();
    }

    @SuppressWarnings("unchecked")
    private static void addToVariablesToRemove(InternalThreadLocalMap threadLocalMap, InternalThreadLocal<?> variable) {

        // 获取需要移除的InternalThreadLocal数据集合
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);

        // 初始化
        Set<InternalThreadLocal<?>> variablesToRemove;
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<InternalThreadLocal<?>, Boolean>());
            threadLocalMap.setIndexedVariable(VARIABLES_TO_REMOVE_INDEX, variablesToRemove);
        } else {
            variablesToRemove = (Set<InternalThreadLocal<?>>)v;
        }

        // 将当前的InternalThreadLocal加入数据集合中，以便线程结束统一清理
        variablesToRemove.add(variable);
    }

    @SuppressWarnings("unchecked")
    private static void removeFromVariablesToRemove(InternalThreadLocalMap threadLocalMap, InternalThreadLocal<?> variable) {

        // 获取需要移除的InternalThreadLocal数据集合
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);

        // 为空不处理
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            return;
        }

        // 从集合中移除当前InternalThreadLocal
        Set<InternalThreadLocal<?>> variablesToRemove = (Set<InternalThreadLocal<?>>) v;
        variablesToRemove.remove(variable);
    }

    /**
     * Returns the current value for the current thread
     */
    @SuppressWarnings("unchecked")
    public final V get() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        Object v = threadLocalMap.indexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            return (V) v;
        }

        return initialize(threadLocalMap);
    }

    private V initialize(InternalThreadLocalMap threadLocalMap) {
        V v = null;
        try {
            v = initialValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        threadLocalMap.setIndexedVariable(index, v);
        addToVariablesToRemove(threadLocalMap, this);
        return v;
    }

    /**
     * Sets the value for the current thread.
     */
    public final void set(V value) {

        // 如果要设置的值是null，或者代表null的UNSET，则移除当前实例对象对应的值
        if (value == null || value == InternalThreadLocalMap.UNSET) {
            remove();
        }

        // 否则正常设置
        else {
            InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();

            // 设置结果到InternalThreadLocalMap中，如果返回true，代表一个当前InternalThreadLocal是一个新的实例
            if (threadLocalMap.setIndexedVariable(index, value)) {
                addToVariablesToRemove(threadLocalMap, this);
            }
        }
    }

    /**
     * Sets the value to uninitialized; a proceeding call to get() will trigger a call to initialValue().
     */
    @SuppressWarnings("unchecked")
    public final void remove() {
        remove(InternalThreadLocalMap.getIfSet());
    }

    /**
     * Sets the value to uninitialized for the specified thread local map;
     * a proceeding call to get() will trigger a call to initialValue().
     * The specified thread local map must be for the current thread.
     */
    @SuppressWarnings("unchecked")
    public final void remove(InternalThreadLocalMap threadLocalMap) {
        if (threadLocalMap == null) {
            return;
        }

        // 从线程上绑定的InternalThreadLocalMap上移除当前InternalThreadLocal
        Object v = threadLocalMap.removeIndexedVariable(index);

        // 从当前线程结束时需要移除的InternalThreadLocal集合中移除当前InternalThreadLocal
        removeFromVariablesToRemove(threadLocalMap, this);

        // 如果当前InternalThreadLocal有设置，则需要进行移除操作的通知
        if (v != InternalThreadLocalMap.UNSET) {
            try {
                onRemoval((V)v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Returns the initial value for this thread-local variable.
     */
    protected V initialValue() throws Exception {
        return null;
    }

    /**
     * Invoked when this thread local variable is removed by {@link #remove()}.
     */
    protected void onRemoval(@SuppressWarnings("unused") V value) throws Exception {
    }
}
