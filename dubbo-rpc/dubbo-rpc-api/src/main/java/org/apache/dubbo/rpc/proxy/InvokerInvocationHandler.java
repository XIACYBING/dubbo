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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerModel;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * InvokerHandler
 */
public class InvokerInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(InvokerInvocationHandler.class);
    private final Invoker<?> invoker;
    private ConsumerModel consumerModel;
    private URL url;

    /**
     * 由serviceKey和协议组成的字符串，比如com.xxx.yyy.AService接口，dubbo协议，group为AGroup，version为3：AGroup/com.xxx.yyy.AService:3:dubbo
     *
     * format:{group}/interfaceName:{version}:protocol
     */
    private String protocolServiceKey;

    public static Field stackTraceField;

    static {
        try {
            stackTraceField = Throwable.class.getDeclaredField("stackTrace");
            ReflectUtils.makeAccessible(stackTraceField);
        } catch (NoSuchFieldException e) {
            // ignore
        }
    }

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
        this.url = invoker.getUrl();
        String serviceKey = this.url.getServiceKey();
        this.protocolServiceKey = this.url.getProtocolServiceKey();
        if (serviceKey != null) {
            this.consumerModel = ApplicationModel.getConsumerModel(serviceKey);
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        // 一般来说，在消费者端，代理对象内部的handler就是当前的InvokerInvocationHandler，然后接口调用会先进入当前invoke方法，再通过网络转发给服务提供者

        // 如果方法的声明着是Object，说明是Object相关方法，直接本地通过invoker调用相关方法即可，不需要转发给服务提供者
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(invoker, args);
        }

        // 获取方法名称和参数类型
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();

        // 无参的toString、$destroy和hashCode方法直接通过invoker调用即可：本地试了一下，会直接调用相关invoker的toString方法，感觉会有问题
        if (parameterTypes.length == 0) {
            if ("toString".equals(methodName)) {
                return invoker.toString();
            } else if ("$destroy".equals(methodName)) {
                invoker.destroy();
                return null;
            } else if ("hashCode".equals(methodName)) {
                return invoker.hashCode();
            }
        }

        // 有参的equals方法，直接invoker进行本地调用
        else if (parameterTypes.length == 1 && "equals".equals(methodName)) {
            return invoker.equals(args[0]);
        }

        // 生成RpcInvocation，记录方法、接口名称、protocolServiceKey（由接口和协议组成的字符串，比如com.xxx.yyy.AService接口，dubbo协议：com.xxx.yyy
        // .AService:dubbo）和参数
        RpcInvocation rpcInvocation = new RpcInvocation(method, invoker.getInterface().getName(), protocolServiceKey, args);

        // 获取serviceKey，格式为：{group}/{interfaceName}:{version}，如果没有指定group和version，那么就只是单纯的接口路径，比如：com.xxx.yyy.AService
        String serviceKey = invoker.getUrl().getServiceKey();

        // 设置为目标服务唯一名称
        rpcInvocation.setTargetServiceUniqueName(serviceKey);

        // invoker.getUrl() returns consumer url.
        // 设置消费者URL
        RpcContext.setRpcContext(invoker.getUrl());

        // 设置消费者和方法model    todo 这是做什么
        if (consumerModel != null) {
            rpcInvocation.put(Constants.CONSUMER_MODEL, consumerModel);
            rpcInvocation.put(Constants.METHOD_MODEL, consumerModel.getMethodModel(method));
        }

        // 通过invoke开始进行远程服务调用，返回Result
        Result result = invoker.invoke(rpcInvocation);

        // 获取结果：抛出异常或返回结果
        return result.recreate();
    }
}
