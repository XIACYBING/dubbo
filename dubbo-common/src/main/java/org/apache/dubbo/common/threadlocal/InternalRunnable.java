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

package org.apache.dubbo.common.threadlocal;


/**
 * {@link InternalRunnable}一般配合{@link NamedInternalThreadFactory#newThread}使用，通过{@link NamedInternalThreadFactory}
 * 生成的线程，在线程执行结束后，都需要调用{@link InternalThreadLocal#removeAll()}把线程上关联的{@link InternalThreadLocal}变量数据移除
 * <p>
 * InternalRunnable
 * There is a risk of memory leak when using {@link InternalThreadLocal} without calling
 * {@link InternalThreadLocal#removeAll()}.
 * This design is learning from {@see io.netty.util.concurrent.FastThreadLocalRunnable} which is in Netty.
 */
public class InternalRunnable implements Runnable{
    private final Runnable runnable;

    public InternalRunnable(Runnable runnable){
        this.runnable=runnable;
    }

    /**
     * After the task execution is completed, it will call {@link InternalThreadLocal#removeAll()} to clear
     * unnecessary variables in the thread.
     */
    @Override
    public void run() {
        try{
            runnable.run();
        }finally {

            // 任务执行完成后，移除线程上关联的所有InternalThreadLocal变量，释放内存
            InternalThreadLocal.removeAll();
        }
    }

    /**
     * Wrap ordinary Runnable into {@link InternalThreadLocal}.
     */
     static Runnable Wrap(Runnable runnable){
        return runnable instanceof InternalRunnable?runnable:new InternalRunnable(runnable);
    }
}
