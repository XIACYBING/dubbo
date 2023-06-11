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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

/**
 * provider端用于切换线程的上下文类加载器的过滤器
 * <p>
 * Set the current execution thread class loader to service interface's class loader.
 */
@Activate(group = CommonConstants.PROVIDER, order = -30000)
public class ClassLoaderFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        // 获取当前线程绑定的类加载器
        ClassLoader ocl = Thread.currentThread().getContextClassLoader();

        // 设置要调用接口的类加载器为线程上下文类加载器
        Thread.currentThread().setContextClassLoader(invoker.getInterface().getClassLoader());
        try {

            // 执行调用
            return invoker.invoke(invocation);
        } finally {

            // 调用完成，恢复类加载器
            Thread.currentThread().setContextClassLoader(ocl);
        }
    }

}
