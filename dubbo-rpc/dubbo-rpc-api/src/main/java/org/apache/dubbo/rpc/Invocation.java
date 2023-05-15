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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.Experimental;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Invocation. (API, Prototype, NonThreadSafe)
 *
 * @serial Don't change the class name and package name.
 * <p>
 * 目标数据的管理：
 * @see #getTargetServiceUniqueName()
 * @see #getServiceName()
 * @see #getMethodName()
 * @see #getParameterTypes()
 * @see #getCompatibleParamSignatures()
 * @see #getArguments()
 * <p>
 * 调用过程管理：
 * @see #getInvoker()
 * @see #getProtocolServiceKey()
 * <p>
 * 属性的管理：
 * @see #put(Object, Object)
 * @see #get(Object)
 * @see #getAttributes()
 * <p>
 * 附件的管理：
 * @see #getAttachments()
 * @see #getObjectAttachments()
 * @see #getAttachment(String)
 * @see #getAttachment(String, String)
 * @see #getObjectAttachment(String)
 * @see #getObjectAttachment(String, Object)
 * @see #setAttachment(String, String)
 * @see #setAttachment(String, Object)
 * @see #setObjectAttachment(String, Object)
 * @see #setAttachmentIfAbsent(String, String)
 * @see #setAttachmentIfAbsent(String, Object)
 * @see #setObjectAttachmentIfAbsent(String, Object)
 * @see org.apache.dubbo.rpc.Invoker#invoke(Invocation)
 * @see org.apache.dubbo.rpc.RpcInvocation
 */
public interface Invocation {

    /**
     * 获取要调用的Service唯一名称
     */
    String getTargetServiceUniqueName();

    String getProtocolServiceKey();

    /**
     * 获取要调用的方法名称
     * <p>
     * get method name.
     *
     * @return method name.
     * @serial
     */
    String getMethodName();

    /**
     * 获取接口名称
     * <p>
     * get the interface name
     *
     * @return
     */
    String getServiceName();

    /**
     * 获取方法参数类型
     * <p>
     * get parameter types.
     *
     * @return parameter types.
     * @serial
     */
    Class<?>[] getParameterTypes();

    /**
     * 获取方法参数签名
     * <p>
     * get parameter's signature, string representation of parameter types.
     *
     * @return parameter's signature
     */
    default String[] getCompatibleParamSignatures() {
        return Stream.of(getParameterTypes())
                .map(Class::getName)
                .toArray(String[]::new);
    }

    /**
     * 获取调用参数数组
     * <p>
     * get arguments.
     *
     * @return arguments.
     * @serial
     */
    Object[] getArguments();

    /**
     * 获取当前所有附件，附件会在Consumer和Provider之间传递
     * <p>
     * get attachments.
     *
     * @return attachments.
     * @serial
     */
    Map<String, String> getAttachments();

    @Experimental("Experiment api for supporting Object transmission")
    Map<String, Object> getObjectAttachments();

    void setAttachment(String key, String value);

    @Experimental("Experiment api for supporting Object transmission")
    void setAttachment(String key, Object value);

    @Experimental("Experiment api for supporting Object transmission")
    void setObjectAttachment(String key, Object value);

    void setAttachmentIfAbsent(String key, String value);

    @Experimental("Experiment api for supporting Object transmission")
    void setAttachmentIfAbsent(String key, Object value);

    @Experimental("Experiment api for supporting Object transmission")
    void setObjectAttachmentIfAbsent(String key, Object value);

    /**
     * get attachment by key.
     *
     * @return attachment value.
     * @serial
     */
    String getAttachment(String key);

    @Experimental("Experiment api for supporting Object transmission")
    Object getObjectAttachment(String key);

    /**
     * get attachment by key with default value.
     *
     * @return attachment value.
     * @serial
     */
    String getAttachment(String key, String defaultValue);

    @Experimental("Experiment api for supporting Object transmission")
    Object getObjectAttachment(String key, Object defaultValue);

    /**
     * 获取当前上下文的invoker
     * <p>
     * get the invoker in current context.
     *
     * @return invoker.
     * @transient
     */
    Invoker<?> getInvoker();

    Object put(Object key, Object value);

    Object get(Object key);

    /**
     * 设置属性，属性不在Consumer和Provider之间流通
     */
    Map<Object, Object> getAttributes();
}