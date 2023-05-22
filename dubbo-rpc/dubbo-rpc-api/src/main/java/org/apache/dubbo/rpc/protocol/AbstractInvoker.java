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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.remoting.Constants.DEFAULT_REMOTING_SERIALIZATION;
import static org.apache.dubbo.remoting.Constants.SERIALIZATION_KEY;
import static org.apache.dubbo.rpc.Constants.SERIALIZATION_ID_KEY;

/**
 * {@link Invoker}的抽象实现，提供{@link #attachment}等属性的管理方法，以及{@link #invoke(Invocation)}方法的通用实现，并提供{@link #doInvoke(Invocation)}方法给子类实现具体的invoke逻辑
 * <p>
 * This Invoker works on Consumer side.
 */
public abstract class AbstractInvoker<T> implements Invoker<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 当前Invoker封装的业务接口类型
     */
    private final Class<T> type;

    /**
     * 代表当前Invoker的URL，一般是provider的url，包含全部配置信息
     */
    private final URL url;

    /**
     * 当前Invoker关联的附件信息，可以来自于{@link #url}
     */
    private final Map<String, Object> attachment;

    /**
     * 当前Invoker的有效标识，和{@link #destroyed}标识相反
     */
    private volatile boolean available = true;

    /**
     * 当前Invoker的销毁标识，和{@link #available}标识相反
     */
    private AtomicBoolean destroyed = new AtomicBoolean(false);

    public AbstractInvoker(Class<T> type, URL url) {
        this(type, url, (Map<String, Object>)null);
    }

    public AbstractInvoker(Class<T> type, URL url, String[] keys) {

        // 从URL上提取对应key的值，形成attachment
        this(type, url, convertAttachment(url, keys));
    }

    public AbstractInvoker(Class<T> type, URL url, Map<String, Object> attachment) {
        if (type == null) {
            throw new IllegalArgumentException("service type == null");
        }
        if (url == null) {
            throw new IllegalArgumentException("service url == null");
        }
        this.type = type;
        this.url = url;
        this.attachment = attachment == null ? null : Collections.unmodifiableMap(attachment);
    }

    private static Map<String, Object> convertAttachment(URL url, String[] keys) {

        // key为空，不处理
        if (ArrayUtils.isEmpty(keys)) {
            return null;
        }
        Map<String, Object> attachment = new HashMap<>();

        // 循环keys数组，从url上提取相应的值，放入attachment
        for (String key : keys) {
            String value = url.getParameter(key);
            if (value != null && value.length() > 0) {
                attachment.put(key, value);
            }
        }

        // 返回attachment
        return attachment;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return available;
    }

    protected void setAvailable(boolean available) {
        this.available = available;
    }

    @Override
    public void destroy() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        setAvailable(false);
    }

    public boolean isDestroyed() {
        return destroyed.get();
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + (getUrl() == null ? "" : getUrl().toString());
    }

    @Override
    public Result invoke(Invocation inv) throws RpcException {

        // 如果当前invoker已经销毁，打印警告日志，但是还是继续invoke
        // if invoker is destroyed due to address refresh from registry, let's allow the current invoke to proceed
        if (destroyed.get()) {
            logger.warn("Invoker for service " + this + " on consumer " + NetUtils.getLocalHost() + " is destroyed, "
                    + ", dubbo version is " + Version.getVersion() + ", this invoker should not be used any longer");
        }

        // invocation关联当前invoke（实际的类型一般是DubboInvoker）
        RpcInvocation invocation = (RpcInvocation) inv;
        invocation.setInvoker(this);

        // 在invocation上附加当前invoker的附件信息
        if (CollectionUtils.isNotEmptyMap(attachment)) {
            invocation.addObjectAttachmentsIfAbsent(attachment);
        }

        // 附加消费上下文的附件内容
        Map<String, Object> contextAttachments = RpcContext.getContext().getObjectAttachments();
        if (CollectionUtils.isNotEmptyMap(contextAttachments)) {
            /**
             * invocation.addAttachmentsIfAbsent(context){@link RpcInvocation#addAttachmentsIfAbsent(Map)}should not be used here,
             * because the {@link RpcContext#setAttachment(String, String)} is passed in the Filter when the call is triggered
             * by the built-in retry mechanism of the Dubbo. The attachment to update RpcContext will no longer work, which is
             * a mistake in most cases (for example, through Filter to RpcContext output traceId and spanId and other information).
             */
            invocation.addObjectAttachments(contextAttachments);
        }

        // 设置InvokeMode：SYNC和ASYNC比较好理解，FUTURE模式则是指直接将承载结果的DefaultFuture返回出去
        invocation.setInvokeMode(RpcUtils.getInvokeMode(url, invocation));

        // 生成请求id，标识本次请求
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);

        // 获取序列化方式的id，默认是hessian2，并放到invocation中
        Byte serializationId = CodecSupport.getIDByName(getUrl().getParameter(SERIALIZATION_KEY, DEFAULT_REMOTING_SERIALIZATION));
        if (serializationId != null) {
            invocation.put(SERIALIZATION_ID_KEY, serializationId);
        }

        AsyncRpcResult asyncResult;
        try {

            // 真正进行invoke逻辑，不同协议有不同实现：dubbo -> DubboInvoker，grpc -> GrpcInvoker，thrift -> ThriftInvoker，
            // inJvm -> InJvmInvoker...
            asyncResult = (AsyncRpcResult) doInvoke(invocation);
        } catch (InvocationTargetException e) { // biz exception
            Throwable te = e.getTargetException();
            if (te == null) {
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
            } else {
                if (te instanceof RpcException) {
                    ((RpcException) te).setCode(RpcException.BIZ_EXCEPTION);
                }
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, te, invocation);
            }
        } catch (RpcException e) {
            if (e.isBiz()) {
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
            } else {
                throw e;
            }
        } catch (Throwable e) {
            asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
        }

        // asyncResult.getResponseFuture()获取到的一般是在HeaderExchangeChannel中构造的DefaultFuture
        // 包装asyncResult为Future，如果调用模式是FUTURE，或外部需要返回Future类型的数据，则直接将此处设置的Future返回
        // 在AsyncRpcResult.recreate会对当前设置的结果进行获取
        RpcContext.getContext().setFuture(new FutureAdapter(asyncResult.getResponseFuture()));
        return asyncResult;
    }

    /**
     * 根据调用模式获取回调处理的线程池
     */
    protected ExecutorService getCallbackExecutor(URL url, Invocation inv) {

        // 获取共享线程池
        ExecutorService sharedExecutor =
            ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension().getExecutor(url);

        // 如果当前调用模式是SYNC/同步，则使用ThreadlessExecutor包装线程池，这会让响应被提交到ThreadlessExecutor，并由当前线程来处理响应
        // SYNC模式下，consumer发起请求后就会通过ThreadlessExecutor线程池阻塞等待响应的返回，阻塞的核心在于ThreadlessExecutor.waitAndDrain()
        if (InvokeMode.SYNC == RpcUtils.getInvokeMode(getUrl(), inv)) {
            return new ThreadlessExecutor(sharedExecutor);
        } else {
            return sharedExecutor;
        }
    }

    protected abstract Result doInvoke(Invocation invocation) throws Throwable;

}
