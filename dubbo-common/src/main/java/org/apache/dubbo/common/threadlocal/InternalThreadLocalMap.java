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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 和{@link ThreadLocal}类似的数据结构，区别在于。{@link InternalThreadLocalMap}采用数组结构进行数据存储，相比于{@link ThreadLocal.ThreadLocalMap}
 * 来说，数据的存储操作效率要高一些
 * <p>
 * The internal data structure that stores the threadLocal variables for Netty and all {@link InternalThread}s.
 * Note that this class is for internal use only. Use {@link InternalThread}
 * unless you know what you are doing.
 */
public final class InternalThreadLocalMap {

    /**
     * 用于存储绑定到当前线程的数据，通过{@link InternalThreadLocal#index}确认数据的存储位置
     * <p>
     * 初始化时，数组大小为{@code 32}，会根据数据存储的情况自增
     */
    private Object[] indexedVariables;

    /**
     * 当使用的线程是原生线程时，还是可以使用{@link InternalThreadLocal}，就是基于当前的{@link #slowThreadLocalMap}提供支持，是一个降级策略
     */
    private static ThreadLocal<InternalThreadLocalMap> slowThreadLocalMap = new ThreadLocal<InternalThreadLocalMap>();

    /**
     * 自增索引，用于计算下次存储到{@link #indexedVariables}数组中的数据的索引位置，静态常量字段
     * <p>
     * 也就是说所有的线程共享这个索引生成器{@link #NEXT_INDEX}，那么对比较后期的线程来说，生成的索引可能很大，需要在{@link #indexedVariables}数组中很后面的地方去设置
     * {@link InternalThreadLocal}的值。
     * <p>
     * 线程副本ThreadLocal是针对所有线程的，而索引是在线程副本创建的时候分配的，所有线程都有可能用某个{@link InternalThreadLocal}，所以对应的索引必须在整个系统唯一
     * <p>
     * 如果{@link #NEXT_INDEX}非静态字段会导致的问题：
     * 如果只是在线程唯一，假设a线程创建线程副本1，分配的索引是0，b线程创建线程副本2，分配的索引也是0，那么b线程在使用线程副本1时，
     * 获取到的值实际上是b线程使用线程副本2时设置进去的值，同样，a线程使用线程副本2时获取到的值是使用线程副本1时设置进去的值
     */
    private static final AtomicInteger NEXT_INDEX = new AtomicInteger();

    /**
     * 当一个与线程绑定的值被删除时，在{@link #indexedVariables}数组中，对应索引位置的值会被设置为{@link #UNSET}
     * <p>
     * {@link #indexedVariables}初始化时，也是使用{@link #UNSET}填充，见{@link #newIndexedVariableTable()}
     */
    public static final Object UNSET = new Object();

    /**
     * 获取绑定在线程上的{@link InternalThreadLocalMap}，可能为空
     */
    public static InternalThreadLocalMap getIfSet() {

        // 获取当前线程的线程实例
        Thread thread = Thread.currentThread();

        // 如果是InternalThread，则获取绑定在上面的InternalThreadLocalMap
        if (thread instanceof InternalThread) {
            return ((InternalThread)thread).threadLocalMap();
        }

        // 否则获取绑定在ThreadLocal中的InternalThreadLocalMap
        return slowThreadLocalMap.get();
    }

    /**
     * 获取绑定在线程上的{@link InternalThreadLocalMap}，如果为空，则初始化一个绑定上去，并返回
     */
    public static InternalThreadLocalMap get() {
        Thread thread = Thread.currentThread();

        if (thread instanceof InternalThread) {
            return fastGet((InternalThread)thread);
        }
        return slowGet();
    }

    /**
     * 移除绑定在线程上的{@link InternalThreadLocalMap}
     */
    public static void remove() {
        Thread thread = Thread.currentThread();

        // 设置InternalThreadLocalMap为空，或者清空slowThreadLocalMap
        if (thread instanceof InternalThread) {
            ((InternalThread)thread).setThreadLocalMap(null);
        } else {
            slowThreadLocalMap.remove();
        }
    }

    public static void destroy() {
        slowThreadLocalMap = null;
    }

    public static int nextVariableIndex() {
        int index = NEXT_INDEX.getAndIncrement();

        // 索引溢出，抛出异常
        if (index < 0) {
            NEXT_INDEX.decrementAndGet();
            throw new IllegalStateException("Too many thread-local indexed variables");
        }
        return index;
    }

    public static int lastVariableIndex() {
        return NEXT_INDEX.get() - 1;
    }

    private InternalThreadLocalMap() {

        // 初始化一个对象数组，作为当前线程存放InternalThreadLocal数据的位置
        indexedVariables = newIndexedVariableTable();
    }

    public Object indexedVariable(int index) {
        Object[] lookup = indexedVariables;
        return index < lookup.length ? lookup[index] : UNSET;
    }

    /**
     * @return {@code true} if and only if a new thread-local variable has been created
     * 返回true代表一个新的{@link InternalThreadLocal}变量被成功创建
     */
    public boolean setIndexedVariable(int index, Object value) {
        Object[] lookup = indexedVariables;
        if (index < lookup.length) {
            Object oldValue = lookup[index];
            lookup[index] = value;

            // 如果原值是UNSET，说明当前是一个新的InternalThreadLocal在设置值
            return oldValue == UNSET;
        } else {
            expandIndexedVariableTableAndSet(index, value);

            // 需要扩容，也说明当前是一个新的ThreadLocal在设置值
            return true;
        }
    }

    public Object removeIndexedVariable(int index) {
        Object[] lookup = indexedVariables;
        if (index < lookup.length) {
            Object v = lookup[index];
            lookup[index] = UNSET;
            return v;
        } else {
            return UNSET;
        }
    }

    public int size() {
        int count = 0;
        for (Object o : indexedVariables) {
            if (o != UNSET) {
                ++count;
            }
        }

        //the fist element in `indexedVariables` is a set to keep all the InternalThreadLocal to remove
        //look at method `addToVariablesToRemove`
        return count - 1;
    }

    private static Object[] newIndexedVariableTable() {
        Object[] array = new Object[32];
        Arrays.fill(array, UNSET);
        return array;
    }

    /**
     * 获取线程上绑定的{@link InternalThreadLocalMap}，如果为空，则初始化一个并绑定到线程上并返回
     */
    private static InternalThreadLocalMap fastGet(InternalThread thread) {
        InternalThreadLocalMap threadLocalMap = thread.threadLocalMap();
        if (threadLocalMap == null) {
            thread.setThreadLocalMap(threadLocalMap = new InternalThreadLocalMap());
        }
        return threadLocalMap;
    }

    /**
     * 获取绑定在{@link #slowThreadLocalMap}上的{@link InternalThreadLocalMap}，如果为空则初始化一个绑定上去并返回
     */
    private static InternalThreadLocalMap slowGet() {
        ThreadLocal<InternalThreadLocalMap> slowThreadLocalMap = InternalThreadLocalMap.slowThreadLocalMap;
        InternalThreadLocalMap ret = slowThreadLocalMap.get();
        if (ret == null) {
            ret = new InternalThreadLocalMap();
            slowThreadLocalMap.set(ret);
        }
        return ret;
    }

    private void expandIndexedVariableTableAndSet(int index, Object value) {

        // 获取旧数组和旧数组容量
        Object[] oldArray = indexedVariables;
        final int oldCapacity = oldArray.length;

        // 计算新数组容量：计算出大于index的，最近一个2的指数，和HashMap的容量计算规则一致
        int newCapacity = index;
        newCapacity |= newCapacity >>> 1;
        newCapacity |= newCapacity >>> 2;
        newCapacity |= newCapacity >>> 4;
        newCapacity |= newCapacity >>> 8;
        newCapacity |= newCapacity >>> 16;
        newCapacity++;

        // 构建新数组
        Object[] newArray = Arrays.copyOf(oldArray, newCapacity);

        // 填充新数组中剩余的位置为UNSET
        Arrays.fill(newArray, oldCapacity, newArray.length, UNSET);

        // 设置当前value
        newArray[index] = value;

        // 设置新数组
        indexedVariables = newArray;
    }
}
