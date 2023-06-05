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
package org.apache.dubbo.common.bytecode;

import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.MAX_PROXY_COUNT;

/**
 * Proxy.
 */

public abstract class Proxy {
    public static final InvocationHandler RETURN_NULL_INVOKER = (proxy, method, args) -> null;
    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);
    private static final String PACKAGE_NAME = Proxy.class.getPackage().getName();

    /**
     * 代理缓存：<类加载器, <代理继承的接口全路径组成的字符串, 用于生成代理对象的Proxy实现类的类实例>>
     * <p>
     * 需要注意的是缓存集合的类型是{@link WeakHashMap}，这意味着key被弱引用维护着，todo 随时可能被回收？
     */
    private static final Map<ClassLoader, Map<String, Object>> PROXY_CACHE_MAP =
        new WeakHashMap<ClassLoader, Map<String, Object>>();

    /**
     * cache class, avoid PermGen OOM.
     * <p>
     * {@link #PROXY_CACHE_MAP}存储的{@link Proxy}实现类的类实例，{@link #PROXY_CLASS_MAP}存储的则是{@link Proxy}实现类的类对象
     * <p>
     * 两个集合中的{@code 类加载器}和{@code 代理继承的接口全路径组成的字符串}都是一致的
     * <p>
     * 代理缓存：<类加载器, <代理继承的接口全路径组成的字符串, 用于生成代理对象的Proxy实现类的类对象>>
     * <p>
     * 生成代理的Proxy实现类的类对象缓存
     */
    private static final Map<ClassLoader, Map<String, Object>> PROXY_CLASS_MAP =
        new WeakHashMap<ClassLoader, Map<String, Object>>();

    /**
     * 生成代理过程中的占位符，如果对象在{@link #PROXY_CACHE_MAP}中获取到当前对象，则需要等待其他线程将相关代理生成并放置到缓存中
     */
    private static final Object PENDING_GENERATION_MARKER = new Object();

    protected Proxy() {
    }

    /**
     * Get proxy.
     *
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(Class<?>... ics) {
        return getProxy(ClassUtils.getClassLoader(Proxy.class), ics);
    }

    /**
     * Get proxy.
     *
     * @param cl  class loader.
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(ClassLoader cl, Class<?>... ics) {

        // 最大实现接口数量限制
        if (ics.length > MAX_PROXY_COUNT) {
            throw new IllegalArgumentException("interface limit exceeded");
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ics.length; i++) {

            // 获取类对象全路径
            String itf = ics[i].getName();

            // 如果类对象非接口，则抛出异常
            if (!ics[i].isInterface()) {
                throw new RuntimeException(itf + " is not a interface.");
            }

            // 通过全路径加载类对象
            Class<?> tmp = null;
            try {
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            // 如果两个类对象不等，说明需要实现的接口类对象对当前类加载器不可见，则无法处理，抛出异常
            if (tmp != ics[i]) {
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");
            }

            // 拼接类对象全路径
            sb.append(itf).append(';');
        }

        // 将所有接口类对象全路径拼接起来，生成一个key，这个key将作为第二层集合的key
        // use interface class name list as key.
        String key = sb.toString();

        // get cache by class loader.
        final Map<String, Object> cache;
        // cache class
        final Map<String, Object> classCache;

        // 获取已缓存的代理，和代理类对象
        // key是ClassLoader对象，一般是线程上关联的ContextClassLoader
        synchronized (PROXY_CACHE_MAP) {
            cache = PROXY_CACHE_MAP.computeIfAbsent(cl, k -> new HashMap<>());
            classCache = PROXY_CLASS_MAP.computeIfAbsent(cl, k -> new HashMap<>());
        }

        Proxy proxy = null;

        // 缓存加锁，限制并发
        synchronized (cache) {
            do {

                // 获取缓存中对象的值，如果是引用类型（一般是SoftReference），则获取引用中的实际对象实例并返回
                Object value = cache.get(key);
                if (value instanceof Reference<?>) {
                    proxy = (Proxy)((Reference<?>)value).get();
                    if (proxy != null) {
                        return proxy;
                    }
                }

                // 根据接口全路径组成的key，获取代理的类对象
                // get Class by key.
                Object clazzObj = classCache.get(key);

                // 如果代理类的类对象为空，或是一个引用类型的，则需要处理
                if (null == clazzObj || clazzObj instanceof Reference<?>) {
                    Class<?> clazz = null;

                    // 如果是引用类型的，则需要获取内部存储的实际代理类的类对象
                    if (clazzObj instanceof Reference<?>) {
                        clazz = (Class<?>)((Reference<?>)clazzObj).get();
                    }

                    // 如果类对象还是空，则需要生成类对象
                    if (null == clazz) {

                        // 如果从代理对象缓存中获取到的value是PENDING_GENERATION_MARKER，说明有其他线程正在处理，调用wait方法等待
                        if (value == PENDING_GENERATION_MARKER) {
                            try {
                                cache.wait();
                            } catch (InterruptedException e) {
                            }
                        }

                        // 否则说明只有当前线程在处理，在缓存集合中放入PENDING_GENERATION_MARKER占位，并防止其他线程并发处理
                        // 放入占位对象后，停止循环，在下面的逻辑生成代理类的类对象和对象实例，并放入缓存中
                        else {
                            cache.put(key, PENDING_GENERATION_MARKER);
                            break;
                        }
                    }

                    // 如果从引用中获取代理类的类对象不为空，则使用反射从类对象中生成一个对象实例并返回
                    else {
                        try {
                            proxy = (Proxy)clazz.newInstance();
                            return proxy;
                        } catch (InstantiationException | IllegalAccessException e) {
                            throw new RuntimeException(e);
                        } finally {

                            // 如果最终生成对象异常，则从缓存集合中移除对应key
                            if (null == proxy) {
                                cache.remove(key);
                            }

                            // 否则将生成的代理对象实例用软引用包装并放入集合中
                            else {
                                cache.put(key, new SoftReference<Proxy>(proxy));
                            }
                        }
                    }
                }
            } while (true);
        }

        // 生成代理类的id，后面将用在包名和类名的生成上
        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;

        // 初始化两个类生成器变量，ccp用于生成代理类，ccm用于生成一个Proxy的实现类，并实现其中的newInstance方法，newInstance方法负责生成代理类（ccp生成的代理类）的类实例
        ClassGenerator ccp = null, ccm = null;
        try {

            // 实例化一个类生成器
            ccp = ClassGenerator.newInstance(cl);

            // 初始化一个方法描述信息集合，用于去重，如果不同接口有相同签名，那么代理类只需要实现一个即可
            Set<String> worked = new HashSet<>();

            // 初始化一个方法集合
            List<Method> methods = new ArrayList<>();

            // 循环接口集合，对于所有非public的接口，需要判断是否处于同一包名下，如果不是，则抛出异常
            // 如果有非public的接口，则将非public接口的报名作为最终包名
            for (int i = 0; i < ics.length; i++) {

                // 当前接口非public
                if (!Modifier.isPublic(ics[i].getModifiers())) {

                    // 获取接口所在的包名
                    String npkg = ics[i].getPackage().getName();

                    // 如果代理类包名为空，则将当前非public的接口包名作为代理类包名
                    if (pkg == null) {
                        pkg = npkg;
                    }

                    // 如果代理类包名不为空，则需要确保非public接口的包名和代理类的包名一致，即两者在同一个包，这样才能互相访问
                    else {
                        if (!pkg.equals(npkg)) {
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                        }
                    }
                }

                // 向类生成器中添加当前接口
                ccp.addInterface(ics[i]);

                // 循环当前接口的所有方法
                for (Method method : ics[i].getMethods()) {

                    // 获取方法的描述信息，即方法前面，比如 int do(int arg1) => "do(I)I"，或 void do(String arg1,boolean arg2) => "do(Ljava/lang/String;Z)V"
                    String desc = ReflectUtils.getDesc(method);

                    // 如果worked集合中已经有当前方法描述信息，或方法是静态的，则不处理
                    if (worked.contains(desc) || Modifier.isStatic(method.getModifiers())) {
                        continue;
                    }

                    // 添加方法描述信息到集合中
                    worked.add(desc);

                    int ix = methods.size();

                    // 获取当前方法的返回值类型和参数类型集合
                    Class<?> rt = method.getReturnType();
                    Class<?>[] pts = method.getParameterTypes();

                    // 生成方法的调用代码
                    // 比如接口下只有方法：int do(int arg)，最终会生成以下信息（这里优化了代码格式，默认生成的代码应该是一行的）
                    // Object[] args = new Object[1];
                    // args[0] = ($w)$1;
                    // Object ret = handler.invoke(this, methods[${methods.size}], args);
                    // return ret==null?(int)0:((Integer)ret).intValue();
                    StringBuilder code =
                        new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    for (int j = 0; j < pts.length; j++) {
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    }
                    code.append(" Object ret = handler.invoke(this, methods[").append(ix).append("], args);");
                    if (!Void.TYPE.equals(rt)) {
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");
                    }

                    // 添加当前方法到方法集合中
                    methods.add(method);

                    // 将生成的方法调用代码添加到类生成器中
                    ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(),
                        code.toString());
                }
            }

            // 如果包名为空，则使用默认包名：Proxy类的包名
            if (pkg == null) {
                pkg = PACKAGE_NAME;
            }

            // create ProxyInstance class.

            // 生成类名，如果使用默认包名，并假设id为1，则最终的类名是：org.apache.dubbo.common.bytecode.proxy1
            String pcn = pkg + ".proxy" + id;

            // 设置类名
            ccp.setClassName(pcn);

            // 设置静态字段：方法数组
            ccp.addField("public static java.lang.reflect.Method[] methods;");

            // 设置处理器字段：InvocationHandler，实现类是InvokerInvocationHandler，内部使用相应的Invoker去完成相关逻辑
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");

            // 添加公共构造器
            // 还是以上面的类型做举例：public proxy1(InvocationHandler arg0) { handler=$1; }
            // todo $1？不应该是arg0吗？
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[] {InvocationHandler.class}, new Class<?>[0],
                "handler=$1;");

            // 添加默认构造器：todo 确认是否默认无参构造器
            ccp.addDefaultConstructor();

            // 生成代理类对象
            Class<?> clazz = ccp.toClass();

            // 向代理类中的静态字段-方法数组字段设置值
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));

            // 到此为止，clazz所对应的代理类的具体代码其实已经生成完成，接下来只需要根据代理类的类名生成相应的对象即可

            // create Proxy class.
            // 创建Proxy的实现类，并实现其中的newInstance方法，用于生成上面逻辑所构造的代理类（clazz）的对象实例
            String fcn = Proxy.class.getName() + id;

            // 生成Proxy实现类的类生成器
            ccm = ClassGenerator.newInstance(cl);

            // 设置类名、默认构造器、父类
            ccm.setClassName(fcn);
            ccm.addDefaultConstructor();
            ccm.setSuperClass(Proxy.class);

            // 实现newInstance方法，通过代理类的类型pcn，去生成代理类的类实例
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn
                + "($1); }");

            // 生成对应的类对象
            Class<?> pc = ccm.toClass();

            // 构建类实例
            proxy = (Proxy)pc.newInstance();

            // 将类实例用软引用包装，放入类缓存中
            synchronized (classCache) {
                classCache.put(key, new SoftReference<Class<?>>(pc));
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {

            // 释放类生成器的资源
            // release ClassGenerator
            if (ccp != null) {
                ccp.release();
            }
            if (ccm != null) {
                ccm.release();
            }

            // 加锁操作，将Proxy实现类的类实例用SoftReference包装并放入缓存中
            synchronized (cache) {
                if (proxy == null) {
                    cache.remove(key);
                } else {
                    cache.put(key, new SoftReference<>(proxy));
                }

                // 通知其他等待线程
                cache.notifyAll();
            }
        }

        // 返回Proxy实现类的类实例
        return proxy;
    }

    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl) {
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            }
            if (Byte.TYPE == cl) {
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            }
            if (Character.TYPE == cl) {
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            }
            if (Double.TYPE == cl) {
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            }
            if (Float.TYPE == cl) {
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            }
            if (Integer.TYPE == cl) {
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            }
            if (Long.TYPE == cl) {
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            }
            if (Short.TYPE == cl) {
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            }
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);
}
