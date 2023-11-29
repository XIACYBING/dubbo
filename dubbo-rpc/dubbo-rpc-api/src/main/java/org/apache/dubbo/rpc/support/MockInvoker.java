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
package org.apache.dubbo.rpc.support;

import com.alibaba.fastjson.JSON;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionFactory;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.rpc.Constants.FAIL_PREFIX;
import static org.apache.dubbo.rpc.Constants.FORCE_PREFIX;
import static org.apache.dubbo.rpc.Constants.MOCK_KEY;
import static org.apache.dubbo.rpc.Constants.RETURN_KEY;
import static org.apache.dubbo.rpc.Constants.RETURN_PREFIX;
import static org.apache.dubbo.rpc.Constants.THROW_PREFIX;

/**
 * 用于Mock服务的Invoker
 */
final public class MockInvoker<T> implements Invoker<T> {
    private static final ProxyFactory PROXY_FACTORY =
        ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    /**
     * MockInvoker的缓存：<Mock配置字符串, MockInvoker>
     */
    private static final Map<String, Invoker<?>> MOCK_MAP = new ConcurrentHashMap<String, Invoker<?>>();

    /**
     * Mock的异常缓存
     */
    private static final Map<String, Throwable> THROWABLE_MAP = new ConcurrentHashMap<String, Throwable>();

    private final URL url;
    private final Class<T> type;

    public MockInvoker(URL url, Class<T> type) {
        this.url = url;
        this.type = type;
    }

    public static Object parseMockValue(String mock) throws Exception {
        return parseMockValue(mock, null);
    }

    public static Object parseMockValue(String mock, Type[] returnTypes) throws Exception {
        Object value = null;

        // 如果是empty字符串，则获取对应的数据，比如基础数据类型的默认值，空数组/集合/字符串等
        if ("empty".equals(mock)) {
            value = ReflectUtils.getEmptyObject(
                returnTypes != null && returnTypes.length > 0 ? (Class<?>)returnTypes[0] : null);
        }

        // null
        else if ("null".equals(mock)) {
            value = null;
        }

        // 布尔值
        else if ("true".equals(mock)) {
            value = true;
        } else if ("false".equals(mock)) {
            value = false;
        }

        // 带双引号或单引号的字符串切割，这里双引号和单引号的判断没有问题
        else if (mock.length() >= 2 && (mock.startsWith("\"") && mock.endsWith("\"")
            || mock.startsWith("\'") && mock.endsWith("\'"))) {
            value = mock.subSequence(1, mock.length() - 1);
        }

        // 正常字符串则直接赋值
        else if (returnTypes != null && returnTypes.length > 0 && returnTypes[0] == String.class) {
            value = mock;
        }

        // 非小数、JSON和List的处理
        else if (StringUtils.isNumeric(mock, false)) {
            value = JSON.parse(mock);
        } else if (mock.startsWith("{")) {
            value = JSON.parseObject(mock, Map.class);
        } else if (mock.startsWith("[")) {
            value = JSON.parseObject(mock, List.class);
        }

        // 不是以上类型的，直接赋值mock
        else {
            value = mock;
        }
        if (ArrayUtils.isNotEmpty(returnTypes)) {
            value = PojoUtils.realize(value, (Class<?>) returnTypes[0], returnTypes.length > 1 ? returnTypes[1] : null);
        }
        return value;
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {

        // 设置当前invoker作为被请求调用的invoker
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation)invocation).setInvoker(this);
        }

        // 获取mock配置，为空则抛出异常
        String mock = getUrl().getMethodParameter(invocation.getMethodName(), MOCK_KEY);
        if (StringUtils.isBlank(mock)) {
            throw new RpcException(new IllegalAccessException("mock can not be null. url :" + url));
        }

        // 规范化mock配置，比如移除force/fail前缀，填充null；具体规则可查看方法注释
        mock = normalizeMock(URL.decode(mock));

        // 如果mock配置是以return作为前缀
        if (mock.startsWith(RETURN_PREFIX)) {

            // 移除return前缀
            mock = mock.substring(RETURN_PREFIX.length()).trim();
            try {

                // 获取方法返回值类型，如果是有泛化对象的，比如Future<?>，则会返回多个，第一个是Future，第二个是具体的泛化对象
                Type[] returnTypes = RpcUtils.getReturnTypes(invocation);

                // 将移除前缀后的mock配置，结合方法返回值类型，转换为具体的mock返回值
                Object value = parseMockValue(mock, returnTypes);

                // 返回结果
                return AsyncRpcResult.newDefaultAsyncResult(value, invocation);
            } catch (Exception ew) {
                throw new RpcException(
                    "mock return invoke error. method :" + invocation.getMethodName() + ", mock:" + mock + ", url: "
                        + url, ew);
            }
        }

        // 如果mock配置是以throw作为前缀
        else if (mock.startsWith(THROW_PREFIX)) {

            // 移除throw前缀
            mock = mock.substring(THROW_PREFIX.length()).trim();

            // 如果为空，抛出默认的RpcException
            if (StringUtils.isBlank(mock)) {
                throw new RpcException("mocked exception for service degradation.");
            }

            // user customized class
            // 否则抛出指定的异常
            else {
                Throwable t = getThrowable(mock);
                throw new RpcException(RpcException.BIZ_EXCEPTION, t);
            }
        }

        // impl mock
        // 如果不是前两者，则默认需要获取对应的invoker进行调用
        else {
            try {
                Invoker<T> invoker = getInvoker(mock);
                return invoker.invoke(invocation);
            } catch (Throwable t) {
                throw new RpcException("Failed to create mock implementation class " + mock, t);
            }
        }
    }

    public static Throwable getThrowable(String throwstr) {

        // 缓存中获取，如果有直接泛化
        Throwable throwable = THROWABLE_MAP.get(throwstr);
        if (throwable != null) {
            return throwable;
        }

        try {

            // 反射获取类对象
            Throwable t;
            Class<?> bizException = ReflectUtils.forName(throwstr);
            Constructor<?> constructor;

            // 获取字符串公开构造器
            constructor = ReflectUtils.findConstructor(bizException, String.class);

            // 实例化
            t = (Throwable)constructor.newInstance(new Object[] {"mocked exception for service degradation."});

            // 加入缓存
            if (THROWABLE_MAP.size() < 1000) {
                THROWABLE_MAP.put(throwstr, t);
            }
            return t;
        } catch (Exception e) {
            throw new RpcException("mock throw error :" + throwstr + " argument error.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Invoker<T> getInvoker(String mockService) {

        // 对应mockService已有MockInvoker，则直接返回
        Invoker<T> invoker = (Invoker<T>)MOCK_MAP.get(mockService);
        if (invoker != null) {
            return invoker;
        }

        // 根据url获取当前接口的类对象
        Class<T> serviceType = (Class<T>)ReflectUtils.forName(url.getServiceInterface());

        // 生成对应的对象实例
        T mockObject = (T)getMockObject(mockService, serviceType);

        // 使用代理工厂，将对象实例包装成Invoker
        invoker = PROXY_FACTORY.getInvoker(mockObject, serviceType, url);

        // 缓存容量足够的情况下，放到MOCK_MAP中
        if (MOCK_MAP.size() < 10000) {
            MOCK_MAP.put(mockService, invoker);
        }
        return invoker;
    }

    @SuppressWarnings("unchecked")
    public static Object getMockObject(String mockService, Class serviceType) {

        // 如果mockService配置是默认配置，则直接将接口名称后加上Mock字符串作为mockService的值
        // 如果不是的话就默认认为是接口配置
        boolean isDefault = ConfigUtils.isDefault(mockService);
        if (isDefault) {
            mockService = serviceType.getName() + "Mock";
        }

        // 加载类对象
        // 如果是默认配置，那么会在接口路径字符串后加上Mock字符串，这种情况下似乎加载不出来任何类对象吧？forName底层也只是用Class.forName去加载类对象
        // 但是从catch的实现来看，应该是要求使用者自己实现一个对应类名称的对象来使用？
        Class<?> mockClass;
        try {
            mockClass = ReflectUtils.forName(mockService);
        } catch (Exception e) {

            // 如果是非默认配置，则获取ExtensionFactory，尝试从对应的第三方容器中获取，比如SpringExtensionFactory会从Spring容器中获取
            // does not check Spring bean if it is default config.
            if (!isDefault) {
                ExtensionFactory extensionFactory =
                    ExtensionLoader.getExtensionLoader(ExtensionFactory.class).getAdaptiveExtension();
                Object obj = extensionFactory.getExtension(serviceType, mockService);
                if (obj != null) {
                    return obj;
                }
            }

            // 如果是默认配置，且无法加载到类对象，说明使用者没有创建对应类对象，抛出异常提示
            throw new IllegalStateException("Did not find mock class or instance " + mockService
                + ", please check if there's mock class or instance implementing interface " + serviceType.getName(),
                e);
        }

        // 校验类对象
        if (mockClass == null || !serviceType.isAssignableFrom(mockClass)) {
            throw new IllegalStateException(
                "The mock class " + mockClass.getName() + " not implement interface " + serviceType.getName());
        }

        // 实例化类对象
        try {
            return mockClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalStateException("No default constructor from mock class " + mockClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * Normalize mock string:
     *
     * <ol>
     * <li>return => return null</li>
     * <li>fail => default</li>
     * <li>force => default</li>
     * <li>fail:throw/return foo => throw/return foo</li>
     * <li>force:throw/return foo => throw/return foo</li>
     * </ol>
     *
     * @param mock mock string
     * @return normalized mock string
     */
    public static String normalizeMock(String mock) {
        if (mock == null) {
            return mock;
        }

        mock = mock.trim();

        if (mock.length() == 0) {
            return mock;
        }

        if (RETURN_KEY.equalsIgnoreCase(mock)) {
            return RETURN_PREFIX + "null";
        }

        if (ConfigUtils.isDefault(mock) || "fail".equalsIgnoreCase(mock) || "force".equalsIgnoreCase(mock)) {
            return "default";
        }

        if (mock.startsWith(FAIL_PREFIX)) {
            mock = mock.substring(FAIL_PREFIX.length()).trim();
        }

        if (mock.startsWith(FORCE_PREFIX)) {
            mock = mock.substring(FORCE_PREFIX.length()).trim();
        }

        if (mock.startsWith(RETURN_PREFIX) || mock.startsWith(THROW_PREFIX)) {
            mock = mock.replace('`', '"');
        }

        return mock;
    }

    @Override
    public URL getUrl() {
        return this.url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
        //do nothing
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }
}
