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
package org.apache.dubbo.common.extension;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.context.Lifecycle;
import org.apache.dubbo.common.extension.support.ActivateComparator;
import org.apache.dubbo.common.extension.support.WrapperComparator;
import org.apache.dubbo.common.lang.Prioritized;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static java.util.ServiceLoader.load;
import static java.util.stream.StreamSupport.stream;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOVE_VALUE_PREFIX;

/**
 *
 * ExtensionLoader是Dubbo SPI的核心实现，是对OCP（微内核 + 插件）的实现，作用类似与JDK SPI的{@link ServiceLoader}
 * <p>
 * 微内核架构，也被成为插件化架构（Plugin-in Architecture），是一种面向功能进行拆分的可拓展性架构，内核功能是比较稳定的，只负责管理插件的生命周期，不会因为系统功能的拓展而不断修改；
 * 而插件模块是独立存在的模块，包含特定的功能，能够拓展内核系统的功能；
 * 内核通常采用Factory、IOC或OSGI等方式管理插件生命周期，而Dubbo决定采用SPI机制来加载插件，Dubbo SPI机制基于JDK原生的SPI机制
 * <p>
 * Dubbo根据SPI配置文件的用途，将其分为三类目录：
 * 1、META-INF/services/目录: 该目录下的SPI配置文件用来兼容JDK SPI，使用{@link ServicesLoadingStrategy}
 * 2、META-INF/dubbo/目录: 该目录用于存放用户自定义的SPI配置文件，使用{@link DubboLoadingStrategy}
 * 3、META-INF/dubbo/internal/目录: 该目录用于存放 Dubbo内部使用的SPI的配置文件，使用{@link DubboInternalLoadingStrategy}
 * <p>
 * Dubbo将SPI配置文件内容改为了KV格式，key是扩展类名称，value是扩展类全路径，这样可以基于URL上的参数，动态切换某些拓展，以及根据{@link SPI#value()}指定默认的扩展
 * <p>
 * KV格式的SPI有以下优势：
 * 1、可以让我们只加载某个拓展类，而不需要像JDK SPI一样将所有拓展类都加载出来
 * 2、可以让我们快速找到需要的扩展类
 * 3、让我们可以根据拓展key更容易定位到问题（加载拓展类时）
 * <p>
 * {@link org.apache.dubbo.rpc.model.ApplicationModel}, {@code DubboBootstrap} and this class are
 * at present designed to be singleton or static (by itself totally static or uses some static fields).
 * So the instances returned from them are of process or classloader scope. If you want to support
 * multiple dubbo servers in a single process, you may need to refactor these three classes.
 * <p>
 * Load dubbo extensions
 * <ul>
 * <li>auto inject dependency extension </li>
 * <li>auto wrap extension in wrapper </li>
 * <li>default extension is an adaptive instance</li>
 * </ul>
 *
 * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">Service Provider in Java 5</a>
 * @see <a href="https://mp.weixin.qq.com/s/GRaXEoTM69V6GI-8v6nnSg">说一说Java、Spring、Dubbo三者SPI机制的原理和区别</a>
 * @see <a href="https://mp.weixin.qq.com/s/o7Ixn2c9z-lQFNNTPTgm3w">面试常问的dubbo的spi机制到底是什么？（上） dubbo 3.0.4</a>
 * @see <a href="https://mp.weixin.qq.com/s/moFWTrqaN5Ur3FnRuLFnIg">面试常问的dubbo的spi机制到底是什么？（下） dubbo 3.0.4</a>
 * @see org.apache.dubbo.common.extension.SPI
 * @see org.apache.dubbo.common.extension.Adaptive
 * @see org.apache.dubbo.common.extension.Activate
 */
public class ExtensionLoader<T> {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);

    private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");

    /**
     * 存储扩展类加载器的Map，key为被{@link SPI}标记的接口，value为对应的{@link ExtensionLoader}实例
     */
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<>(64);

    /**
     * 存储扩展实现类和对应实例的Map，key为扩展类的类对象，value为扩展类的类实例
     */
    private static final ConcurrentMap<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<>(64);

    /**
     * 实例字段，表示当前{@link ExtensionLoader}要加载的{@link SPI}接口
     */
    private final Class<?> type;

    private final ExtensionFactory objectFactory;

    /**
     * 实例字段，key为拓展类在SPI中的key，value为拓展类的类对象，和{@link #cachedClasses}正好相反
     */
    private final ConcurrentMap<Class<?>, String> cachedNames = new ConcurrentHashMap<>();

    /**
     * 实例字段，key为拓展类的类对象，value为拓展类在SPI中的key，和{@link #cachedNames}正好相反
     */
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();

    /**
     * 实例字段，缓存被{@link Activate}或{@link com.alibaba.dubbo.common.extension.Activate}标注的拓展实现类
     * key是拓展实现类的key，value是{@link org.apache.dubbo.common.extension.Activate}或
     * {@link com.alibaba.dubbo.common.extension.Activate}注解的实例
     */
    private final Map<String, Object> cachedActivates = new ConcurrentHashMap<>();

    /**
     * 实例字段，key为拓展类在SPI中的key，value为拓展类实例
     */
    private final ConcurrentMap<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<>();

    /**
     * 适配器类对象
     * <ol>
     *     <li>如果不为空，说明是用户通过{@link Adaptive}注解手动指定的</li>
     *     <li>如果为空，则需要通过{@link #createAdaptiveExtension()}获得</li>
     * </ol>
     */
    private volatile Class<?> cachedAdaptiveClass = null;
    private final Holder<Object> cachedAdaptiveInstance = new Holder<>();

    /**
     * 实例字段，代表当前{@link SPI}接口的默认值，即默认拓展
     */
    private String cachedDefaultName;
    private volatile Throwable createAdaptiveInstanceError;

    /**
     * 如果SPI实现有一个{@link #type}作为入参的构造器，则认为是一个包装拓展，需要缓存到当前集合中
     */
    private Set<Class<?>> cachedWrapperClasses;

    private Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<>();

    /**
     * Record all unacceptable exceptions when using SPI
     */
    private Set<String> unacceptableExceptions = new ConcurrentHashSet<>();

    /**
     * 加载策略数组，{@link LoadingStrategy}内置三种实现：{@link DubboInternalLoadingStrategy}、{@link DubboLoadingStrategy}
     * 和{@link ServicesLoadingStrategy}，三种加载策略对应着三种SPI配置目录
     * <p>
     * 当前字段是类字段，说明每次修改都会被所有拓展类应用
     */
    private static volatile LoadingStrategy[] strategies = loadLoadingStrategies();

    public static void setLoadingStrategies(LoadingStrategy... strategies) {
        if (ArrayUtils.isNotEmpty(strategies)) {
            ExtensionLoader.strategies = strategies;
        }
    }

    /**
     * Load all {@link Prioritized prioritized} {@link LoadingStrategy Loading Strategies} via {@link ServiceLoader}
     *
     * @return non-null
     * @since 2.7.7
     */
    private static LoadingStrategy[] loadLoadingStrategies() {
        return stream(load(LoadingStrategy.class).spliterator(), false)
                .sorted()
                .toArray(LoadingStrategy[]::new);
    }

    /**
     * Get all {@link LoadingStrategy Loading Strategies}
     *
     * @return non-null
     * @see LoadingStrategy
     * @see Prioritized
     * @since 2.7.7
     */
    public static List<LoadingStrategy> getLoadingStrategies() {
        return asList(strategies);
    }

    private ExtensionLoader(Class<?> type) {
        this.type = type;
        objectFactory =
                (type == ExtensionFactory.class ? null : ExtensionLoader.getExtensionLoader(ExtensionFactory.class).getAdaptiveExtension());
    }

    private static <T> boolean withExtensionAnnotation(Class<T> type) {
        return type.isAnnotationPresent(SPI.class);
    }

    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {

        // 类对象检查
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type (" + type + ") is not an interface!");
        }
        if (!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException(
                "Extension type (" + type + ") is not an extension, because it is NOT annotated with @"
                    + SPI.class.getSimpleName() + "!");
        }

        // 判断对应SPI类是否已经有对应的ExtensionLoader
        ExtensionLoader<T> loader = (ExtensionLoader<T>)EXTENSION_LOADERS.get(type);

        // 没有的化就进行ExtensionLoader的实例化，并放入内存中
        if (loader == null) {
            EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<T>(type));
            loader = (ExtensionLoader<T>)EXTENSION_LOADERS.get(type);
        }

        // 返回加载完成的ExtensionLoader
        return loader;
    }

    // For testing purposes only
    @Deprecated
    public static void resetExtensionLoader(Class type) {
        ExtensionLoader loader = EXTENSION_LOADERS.get(type);
        if (loader != null) {
            // Remove all instances associated with this loader as well
            Map<String, Class<?>> classes = loader.getExtensionClasses();
            for (Map.Entry<String, Class<?>> entry : classes.entrySet()) {
                EXTENSION_INSTANCES.remove(entry.getValue());
            }
            classes.clear();
            EXTENSION_LOADERS.remove(type);
        }
    }

    // only for unit test
    @Deprecated
    public static void destroyAll() {
        EXTENSION_INSTANCES.forEach((_type, instance) -> {
            if (instance instanceof Lifecycle) {
                Lifecycle lifecycle = (Lifecycle) instance;
                try {
                    lifecycle.destroy();
                } catch (Exception e) {
                    logger.error("Error destroying extension " + lifecycle, e);
                }
            }
        });
        EXTENSION_INSTANCES.clear();
        EXTENSION_LOADERS.clear();
    }

    private static ClassLoader findClassLoader() {
        return ClassUtils.getClassLoader(ExtensionLoader.class);
    }

    public String getExtensionName(T extensionInstance) {
        return getExtensionName(extensionInstance.getClass());
    }

    public String getExtensionName(Class<?> extensionClass) {
        getExtensionClasses();// load class
        return cachedNames.get(extensionClass);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, key, null)}
     *
     * @param url url
     * @param key url parameter key which used to get extension point names
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String, String)
     */
    public List<T> getActivateExtension(URL url, String key) {
        return getActivateExtension(url, key, null);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, values, null)}
     *
     * @param url    url
     * @param values extension point names
     * @return extension list which are activated
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String[] values) {
        return getActivateExtension(url, values, null);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, url.getParameter(key).split(","), null)}
     *
     * @param url   url
     * @param key   url parameter key which used to get extension point names
     * @param group group
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String key, String group) {
        String value = url.getParameter(key);
        return getActivateExtension(url, StringUtils.isEmpty(value) ? null : COMMA_SPLIT_PATTERN.split(value), group);
    }

    /**
     * 获取被{@link Activate}或{@link com.alibaba.dubbo.common.extension.Activate}注解标注的拓展实现类
     * <p>
     * Get activate extensions.
     *
     * @param url    url
     * @param values extension point names
     * @param group  group
     * @return extension list which are activated
     * @see org.apache.dubbo.common.extension.Activate
     */
    public List<T> getActivateExtension(URL url, String[] values, String group) {
        List<T> activateExtensions = new ArrayList<>();
        // solve the bug of using @SPI's wrapper method to report a null pointer exception.
        TreeMap<Class, T> activateExtensionsMap = new TreeMap<>(ActivateComparator.COMPARATOR);
        Set<String> loadedNames = new HashSet<>();
        Set<String> names = CollectionUtils.ofSet(values);
        if (!names.contains(REMOVE_VALUE_PREFIX + DEFAULT_KEY)) {

            // 加载拓展类的类对象
            getExtensionClasses();

            // 循环已缓存的激活拓展类配置，根据注解配置的group、value和一些其他参数，获取需要的激活拓展类
            for (Map.Entry<String, Object> entry : cachedActivates.entrySet()) {
                String name = entry.getKey();
                Object activate = entry.getValue();

                String[] activateGroup, activateValue;

                // 根据注解实例类型，获取注解上的group和value值
                if (activate instanceof Activate) {
                    activateGroup = ((Activate)activate).group();
                    activateValue = ((Activate)activate).value();
                } else if (activate instanceof com.alibaba.dubbo.common.extension.Activate) {
                    activateGroup = ((com.alibaba.dubbo.common.extension.Activate)activate).group();
                    activateValue = ((com.alibaba.dubbo.common.extension.Activate)activate).value();
                }

                // 其他的值不处理：也不会有其他的值
                else {
                    continue;
                }

                if (
                    // 匹配激活的group：即provider端还是consumer端
                    isMatchGroup(group, activateGroup)

                        // 匹配入参中的拓展名：如果当前拓展名不在入参values代表的拓展名集合中，才需要处理
                        && !names.contains(name)

                        // 匹配入参中的拓展名：如果names中包含 -name，则表示当前activate注解对应的拓展实现类不激活
                        && !names.contains(REMOVE_VALUE_PREFIX + name)

                        // 检查url参数中是否包含指定的key
                        && isActive(activateValue, url)

                        // 如果当前拓展实现类已经在集合中，就不二次处理了
                        && !loadedNames.contains(name)) {

                    // 将拓展类的类对象和实例对象放入集合中
                    activateExtensionsMap.put(getExtensionClass(name), getExtension(name));

                    // 将当前拓展类的key放入loadedNames集合中
                    loadedNames.add(name);
                }
            }

            // 如果循环结束，获取到的激活拓展类集合不为空，则将激活拓展类实例放入activateExtensions集合中
            if (!activateExtensionsMap.isEmpty()) {
                activateExtensions.addAll(activateExtensionsMap.values());
            }
        }

        // 对于入参传入的values集合中包含的拓展类key，判断是否需要加入到activateExtensions集合中
        List<T> loadedExtensions = new ArrayList<>();
        for (String name : names) {

            // 如果name以-开始，或names集合中包含-name，则当前name不处理
            if (!name.startsWith(REMOVE_VALUE_PREFIX) && !names.contains(REMOVE_VALUE_PREFIX + name)) {

                // 当前name不在之前的cachedActivates循环中处理过
                if (!loadedNames.contains(name)) {

                    // 如果当前name是default，且之前加载的loadedExtensions集合不为空，则将loadedExtensions集合中所有的拓展类实例加入到activateExtensions的开头
                    if (DEFAULT_KEY.equals(name)) {
                        if (!loadedExtensions.isEmpty()) {
                            activateExtensions.addAll(0, loadedExtensions);
                            loadedExtensions.clear();
                        }
                    }

                    // 当前name不是default，则将对应的拓展类实例加入loadedExtensions集合中
                    else {
                        loadedExtensions.add(getExtension(name));
                    }

                    // 将当前name加入到loadedNames集合中
                    loadedNames.add(name);
                }

                // 当前name已经被处理过
                else {
                    // If getExtension(name) exists, getExtensionClass(name) must exist, so there is no null pointer processing here.
                    String simpleName = getExtensionClass(name).getSimpleName();
                    logger.warn(
                        "Catch duplicated filter, ExtensionLoader will ignore one of them. Please check. Filter Name: "
                            + name + ". Ignored Class Name: " + simpleName);
                }
            }
        }

        // 循环结束，如果loadedExtensions中还有拓展类实例，则加入activateExtensions集合中
        if (!loadedExtensions.isEmpty()) {
            activateExtensions.addAll(loadedExtensions);
        }

        // 返回activateExtensions集合
        return activateExtensions;
    }

    private boolean isMatchGroup(String group, String[] groups) {
        if (StringUtils.isEmpty(group)) {
            return true;
        }
        if (groups != null && groups.length > 0) {
            for (String g : groups) {
                if (group.equals(g)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isActive(String[] keys, URL url) {

        // 未配置的直接返回true
        if (keys.length == 0) {
            return true;
        }

        // 循环key数组，只要url上包含任意一个key，就返回true
        for (String key : keys) {
            // @Active(value="key1:value1, key2:value2")
            String keyValue = null;

            // 如果key是key:value格式的，则需要其切割开，且在匹配时要求url上对应的参数key和value和配置的一致
            if (key.contains(":")) {
                String[] arr = key.split(":");
                key = arr[0];
                keyValue = arr[1];
            }

            // 循环url上的参数集合，判断是否符合要求
            for (Map.Entry<String, String> entry : url.getParameters().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();

                // key：url上包含对应key，或包含xxx.key  todo 后者感觉会导致一些bug
                if ((k.equals(key) || k.endsWith("." + key))

                    // value：如果配置的value不为空，则要求url上对应参数的value和配置的value一致；配置的value如果为空，则要求url上对应的参数value不能为配置定义的空（不能为空字符串、false、0、null或N/A）
                    && ((keyValue != null && keyValue.equals(v)) || (keyValue == null && ConfigUtils.isNotEmpty(v)))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get extension's instance. Return <code>null</code> if extension is not found or is not initialized. Pls. note
     * that this method will not trigger extension load.
     * <p>
     * In order to trigger extension load, call {@link #getExtension(String)} instead.
     *
     * @see #getExtension(String)
     */
    @SuppressWarnings("unchecked")
    public T getLoadedExtension(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        Holder<Object> holder = getOrCreateHolder(name);
        return (T) holder.get();
    }

    private Holder<Object> getOrCreateHolder(String name) {

        // 获取对应实例的Holder
        Holder<Object> holder = cachedInstances.get(name);

        // 为空就创建一个Holder放入内存中
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<>());
            holder = cachedInstances.get(name);
        }

        // 返回获取到的Holder
        return holder;
    }

    /**
     * Return the list of extensions which are already loaded.
     * <p>
     * Usually {@link #getSupportedExtensions()} should be called in order to get all extensions.
     *
     * @see #getSupportedExtensions()
     */
    public Set<String> getLoadedExtensions() {
        return Collections.unmodifiableSet(new TreeSet<>(cachedInstances.keySet()));
    }

    public List<T> getLoadedExtensionInstances() {
        List<T> instances = new ArrayList<>();
        cachedInstances.values().forEach(holder -> instances.add((T) holder.get()));
        return instances;
    }

    public Object getLoadedAdaptiveExtensionInstances() {
        return cachedAdaptiveInstance.get();
    }

    /**
     * Find the extension with the given name. If the specified name is not found, then {@link IllegalStateException}
     * will be thrown.
     */
    @SuppressWarnings("unchecked")
    public T getExtension(String name) {
        return getExtension(name, true);
    }

    public T getExtension(String name, boolean wrap) {

        // 名称校验
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }

        // 获取默认拓展类：其实最后也是将默认拓展类key传入当前方法
        if ("true".equals(name)) {
            return getDefaultExtension();
        }

        // 获取或创建对应拓展类实例的Holder：注意，当前方法只是创建或获取Holder，如果是第一次获取对应拓展类，则返回的Holder中实例会为空
        final Holder<Object> holder = getOrCreateHolder(name);

        // 如果实例为空，则Double Check，再次创建实例，并将创建完成的实例设置到Holder中
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {

                    // 创建拓展类实例
                    instance = createExtension(name, wrap);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * get the original type.
     * @param name
     * @return
     */
    public T getOriginalInstance(String name) {
        getExtension(name);
        Class<?> clazz = getExtensionClasses().get(name);
        return (T) EXTENSION_INSTANCES.get(clazz);
    }

    /**
     * Get the extension by specified name if found, or {@link #getDefaultExtension() returns the default one}
     *
     * @param name the name of extension
     * @return non-null
     */
    public T getOrDefaultExtension(String name) {
        return containsExtension(name) ? getExtension(name) : getDefaultExtension();
    }

    /**
     * Return default extension, return <code>null</code> if it's not configured.
     */
    public T getDefaultExtension() {

        // 加载默认拓展时，实现类Class集合可能为空，需要加载
        getExtensionClasses();

        // 没有默认实现
        if (StringUtils.isBlank(cachedDefaultName) || "true".equals(cachedDefaultName)) {
            return null;
        }

        // 调用getExtension，传入默认拓展类名称
        return getExtension(cachedDefaultName);
    }

    public boolean hasExtension(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        Class<?> c = this.getExtensionClass(name);
        return c != null;
    }

    public Set<String> getSupportedExtensions() {
        Map<String, Class<?>> clazzes = getExtensionClasses();
        return Collections.unmodifiableSet(new TreeSet<>(clazzes.keySet()));
    }

    public Set<T> getSupportedExtensionInstances() {
        List<T> instances = new LinkedList<>();
        Set<String> supportedExtensions = getSupportedExtensions();
        if (CollectionUtils.isNotEmpty(supportedExtensions)) {
            for (String name : supportedExtensions) {
                instances.add(getExtension(name));
            }
        }
        // sort the Prioritized instances
        sort(instances, Prioritized.COMPARATOR);
        return new LinkedHashSet<>(instances);
    }

    /**
     * Return default extension name, return <code>null</code> if not configured.
     */
    public String getDefaultExtensionName() {
        getExtensionClasses();
        return cachedDefaultName;
    }

    /**
     * Register new extension via API
     *
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension with the same name has already been registered.
     */
    public void addExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + " doesn't implement the Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + " can't be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " already exists (Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
        } else {
            if (cachedAdaptiveClass != null) {
                throw new IllegalStateException("Adaptive Extension already exists (Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
        }
    }

    /**
     * Replace the existing extension via API
     *
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension to be placed doesn't exist
     * @deprecated not recommended any longer, and use only when test
     */
    @Deprecated
    public void replaceExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + " doesn't implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + " can't be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (!cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " doesn't exist (Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
            cachedInstances.remove(name);
        } else {
            if (cachedAdaptiveClass == null) {
                throw new IllegalStateException("Adaptive Extension doesn't exist (Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
            cachedAdaptiveInstance.set(null);
        }
    }

    @SuppressWarnings("unchecked")
    public T getAdaptiveExtension() {
        Object instance = cachedAdaptiveInstance.get();
        if (instance == null) {
            if (createAdaptiveInstanceError != null) {
                throw new IllegalStateException("Failed to create adaptive instance: " +
                        createAdaptiveInstanceError.toString(),
                        createAdaptiveInstanceError);
            }

            synchronized (cachedAdaptiveInstance) {
                instance = cachedAdaptiveInstance.get();
                if (instance == null) {
                    try {
                        instance = createAdaptiveExtension();
                        cachedAdaptiveInstance.set(instance);
                    } catch (Throwable t) {
                        createAdaptiveInstanceError = t;
                        throw new IllegalStateException("Failed to create adaptive instance: " + t.toString(), t);
                    }
                }
            }
        }

        return (T) instance;
    }

    private IllegalStateException findException(String name) {
        StringBuilder buf = new StringBuilder("No such extension " + type.getName() + " by name " + name);

        int i = 1;
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (entry.getKey().toLowerCase().startsWith(name.toLowerCase())) {
                if (i == 1) {
                    buf.append(", possible causes: ");
                }
                buf.append("\r\n(");
                buf.append(i++);
                buf.append(") ");
                buf.append(entry.getKey());
                buf.append(":\r\n");
                buf.append(StringUtils.toString(entry.getValue()));
            }
        }

        if (i == 1) {
            buf.append(", no related exception was found, please check whether related SPI module is missing.");
        }
        return new IllegalStateException(buf.toString());
    }

    @SuppressWarnings("unchecked")
    private T createExtension(String name, boolean wrap) {

        // 根据拓展类的key获取拓展类类实例，并检查：todo 如果是用户自定义拓展类，第一次加载的时候，不是应该只加载对应拓展类的全路径吗？此处的get应该返回一个字符串才对
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null || unacceptableExceptions.contains(name)) {
            throw findException(name);
        }
        try {

            // 从内存中获取拓展类实例
            T instance = (T)EXTENSION_INSTANCES.get(clazz);

            // 如果为空，就通过反射创建拓展类实例，并放入内存中
            if (instance == null) {
                EXTENSION_INSTANCES.putIfAbsent(clazz, clazz.getDeclaredConstructor().newInstance());
                instance = (T)EXTENSION_INSTANCES.get(clazz);
            }

            // 对应拓展类相互依赖时的处理，即当前拓展类如果依赖其他拓展类，则需要注入：仅支持setter方法
            injectExtension(instance);

            // 如果需要包装拓展类实例，则进行包装
            if (wrap) {

                List<Class<?>> wrapperClassesList = new ArrayList<>();
                if (cachedWrapperClasses != null) {
                    wrapperClassesList.addAll(cachedWrapperClasses);
                    wrapperClassesList.sort(WrapperComparator.COMPARATOR);
                    Collections.reverse(wrapperClassesList);
                }

                // 通过Wrapper注解配置，实例化wrapper，包装拓展类实例：包装类也会实现拓展类接口，可以将不同拓展实现类的相同逻辑抽取到包装类中，然后通过包装类注解指定是否要服用，基于包装类，完成相关可复用逻辑的调用
                if (CollectionUtils.isNotEmpty(wrapperClassesList)) {
                    for (Class<?> wrapperClass : wrapperClassesList) {
                        Wrapper wrapper = wrapperClass.getAnnotation(Wrapper.class);
                        if (wrapper == null || (ArrayUtils.contains(wrapper.matches(), name) && !ArrayUtils.contains(
                            wrapper.mismatches(), name))) {
                            instance = injectExtension((T)wrapperClass.getConstructor(type).newInstance(instance));
                        }
                    }
                }
            }

            // 初始化拓展类：通过Lifecycle接口进行生命周期管理，当前主要是调用initialize方法
            initExtension(instance);
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException(
                "Extension instance (name: " + name + ", class: " + type + ") couldn't be instantiated: "
                    + t.getMessage(), t);
        }
    }

    private boolean containsExtension(String name) {
        return getExtensionClasses().containsKey(name);
    }

    /**
     * 拓展注入：仅支持setter方法
     */
    private T injectExtension(T instance) {

        // 如果objectFactory/对象工厂为空，则无法进行注入，直接返回
        if (objectFactory == null) {
            return instance;
        }

        try {

            // 循环当前实例的类对象的所有方法，获取到需要注入的方法，并进行注入
            for (Method method : instance.getClass().getMethods()) {
                if (!isSetter(method)) {
                    continue;
                }
                /*
                 * Check {@link DisableInject} to see if we need auto-injection for this property
                 */
                if (method.getAnnotation(DisableInject.class) != null) {
                    continue;
                }
                Class<?> pt = method.getParameterTypes()[0];
                if (ReflectUtils.isPrimitives(pt)) {
                    continue;
                }

                try {

                    // setProperty：通过方法名称获取property属性
                    String property = getSetterProperty(method);

                    // 通过objectFactory（ExtensionFactory的实现），获取对应的拓展类，以实现不同拓展类互相依赖的特性
                    Object object = objectFactory.getExtension(pt, property);

                    // 获取到的拓展类不为空，则通过反射调用方法进行设置
                    if (object != null) {
                        method.invoke(instance, object);
                    }
                } catch (Exception e) {
                    logger.error("Failed to inject via method " + method.getName()
                            + " of interface " + type.getName() + ": " + e.getMessage(), e);
                }

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return instance;
    }

    private void initExtension(T instance) {
        if (instance instanceof Lifecycle) {
            Lifecycle lifecycle = (Lifecycle) instance;
            lifecycle.initialize();
        }
    }

    /**
     * get properties name for setter, for instance: setVersion, return "version"
     * <p>
     * return "", if setter name with length less than 3
     */
    private String getSetterProperty(Method method) {
        return method.getName().length() > 3 ? method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4) : "";
    }

    /**
     * return true if and only if:
     * <p>
     * 1, public
     * <p>
     * 2, name starts with "set"
     * <p>
     * 3, only has one parameter
     */
    private boolean isSetter(Method method) {
        return method.getName().startsWith("set")
                && method.getParameterTypes().length == 1
                && Modifier.isPublic(method.getModifiers());
    }

    private Class<?> getExtensionClass(String name) {
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        if (name == null) {
            throw new IllegalArgumentException("Extension name == null");
        }
        return getExtensionClasses().get(name);
    }

    private Map<String, Class<?>> getExtensionClasses() {

        // 获取扩展类实现类的集合
        Map<String, Class<?>> classes = cachedClasses.get();

        // Double Check
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();

                // 还是为空，加载对应类对象，会根据不同策略加载
                if (classes == null) {
                    classes = loadExtensionClasses();
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    /**
     * synchronized in getExtensionClasses
     */
    private Map<String, Class<?>> loadExtensionClasses() {

        // 缓存默认拓展的拓展类key
        cacheDefaultExtensionName();

        Map<String, Class<?>> extensionClasses = new HashMap<>();

        // 根据不同策略，加载不同目录下的拓展类
        for (LoadingStrategy strategy : strategies) {

            // 加载当前接口下的所有SPI配置
            loadDirectory(extensionClasses, strategy.directory(), type.getName(), strategy.preferExtensionClassLoader(),
                strategy.overridden(), strategy.excludedPackages());

            // 如果是apache包下的接口，则同时也加载alibaba包下的相同接口
            loadDirectory(extensionClasses, strategy.directory(), type.getName().replace("org.apache", "com.alibaba"),
                strategy.preferExtensionClassLoader(), strategy.overridden(), strategy.excludedPackages());
        }

        return extensionClasses;
    }

    /**
     * extract and cache default extension name if exists
     */
    private void cacheDefaultExtensionName() {
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if (defaultAnnotation == null) {
            return;
        }

        String value = defaultAnnotation.value();
        if ((value = value.trim()).length() > 0) {
            String[] names = NAME_SEPARATOR.split(value);
            if (names.length > 1) {
                throw new IllegalStateException("More than 1 default extension name on extension " + type.getName()
                        + ": " + Arrays.toString(names));
            }
            if (names.length == 1) {
                cachedDefaultName = names[0];
            }
        }
    }

    private void loadDirectory(Map<String, Class<?>> extensionClasses, String dir, String type) {
        loadDirectory(extensionClasses, dir, type, false, false);
    }

    /**
     * 读取{@code dir}目录下的SPI配置文件，加载相关配置，进行相关判断，并放入{@code extensionClasses}集合中
     */
    private void loadDirectory(Map<String, Class<?>> extensionClasses, String dir, String type,
        boolean extensionLoaderClassLoaderFirst, boolean overridden, String... excludedPackages) {

        // 组成资源路径
        // dir：META-INF/dubbo/internal/
        //      META-INF/dubbo/
        //      META-INF/services/
        //      META-INF/dubbo/external/    todo external的实现类都是测试类，不确定实际有没有
        String fileName = dir + type;
        try {
            Enumeration<java.net.URL> urls = null;

            // 获取类加载器
            ClassLoader classLoader = findClassLoader();

            // 是否优先使用加载ExtensionLoader类对象的类加载器加载SPI配置资源
            // try to load from ExtensionLoader's ClassLoader first
            if (extensionLoaderClassLoaderFirst) {
                ClassLoader extensionLoaderClassLoader = ExtensionLoader.class.getClassLoader();
                if (ClassLoader.getSystemClassLoader() != extensionLoaderClassLoader) {
                    urls = extensionLoaderClassLoader.getResources(fileName);
                }
            }

            // 如果urls为null，或没有数据，则继续尝试使用指定的classLoader或系统ClassLoader来加载资源
            if (urls == null || !urls.hasMoreElements()) {
                if (classLoader != null) {
                    urls = classLoader.getResources(fileName);
                } else {
                    urls = ClassLoader.getSystemResources(fileName);
                }
            }

            if (urls != null) {

                // 循环加载到的文件资源
                while (urls.hasMoreElements()) {
                    java.net.URL resourceURL = urls.nextElement();

                    // 解析文件资源，获取到对应的key和类全限定路径，并加载类对象
                    loadResource(extensionClasses, classLoader, resourceURL, overridden, excludedPackages);
                }
            }
        } catch (Throwable t) {
            logger.error("Exception occurred when loading extension class (interface: " + type + ", description file: "
                + fileName + ").", t);
        }
    }

    /**
     * 读取{@code resourceURL}文件对应的内容，加载其中合适的SPI资源，并放入{@code extensionClasses}集合中
     */
    private void loadResource(Map<String, Class<?>> extensionClasses, ClassLoader classLoader, java.net.URL resourceURL,
        boolean overridden, String... excludedPackages) {
        try {

            // 读取文件资源
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resourceURL.openStream(), StandardCharsets.UTF_8))) {
                String line;
                String clazz = null;
                while ((line = reader.readLine()) != null) {

                    // 只解析#号之前的
                    final int ci = line.indexOf('#');
                    if (ci >= 0) {
                        line = line.substring(0, ci);
                    }
                    line = line.trim();
                    if (line.length() > 0) {
                        try {
                            String name = null;

                            // 等号之前的是key，之后的是类全限定路径，如果没有等号，则直接认为是类全限定路径
                            int i = line.indexOf('=');
                            if (i > 0) {
                                name = line.substring(0, i).trim();
                                clazz = line.substring(i + 1).trim();
                            } else {
                                clazz = line;
                            }

                            // 类全限定路径不为空，且不在指定排除的包下，则加载类全限定路径对应的类对象
                            if (StringUtils.isNotEmpty(clazz) && !isExcluded(clazz, excludedPackages)) {

                                // 加载类对象
                                Class<?> acClazz = Class.forName(clazz, true, classLoader);

                                // 处理类对象
                                loadClass(extensionClasses, resourceURL, acClazz, name, overridden);
                            }
                        } catch (Throwable t) {
                            IllegalStateException e = new IllegalStateException(
                                    "Failed to load extension class (interface: " + type + ", class line: " + line + ") in " + resourceURL +
                                            ", cause: " + t.getMessage(), t);
                            exceptions.put(line, e);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("Exception occurred when loading extension class (interface: " +
                    type + ", class file: " + resourceURL + ") in " + resourceURL, t);
        }
    }

    private boolean isExcluded(String className, String... excludedPackages) {
        if (excludedPackages != null) {
            for (String excludePackage : excludedPackages) {
                if (className.startsWith(excludePackage + ".")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 加载class：
     * <ol>
     *     <li>适配器判断</li>
     *     <li>包装类判断</li>
     *     <li>构造器判断</li>
     *     <li>key判断</li>
     *     <li>类名判断</li>
     *     <li>将类放入{@code extensionClasses}</li>
     * </ol>
     */
    private void loadClass(Map<String, Class<?>> extensionClasses, java.net.URL resourceURL, Class<?> clazz,
        String name, boolean overridden) throws NoSuchMethodException {

        // 类对象不是当前接口的子类
        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException(
                "Error occurred when loading extension class (interface: " + type + ", class line: " + clazz.getName()
                    + "), class " + clazz.getName() + " is not subtype of interface.");
        }

        // 类对象有Adaptive注解，则进行自适应类的相关判断和设置：如果没有自适应类，则当前类对象直接设置为自适应类，如果已有，但是overriden为true，则进行覆盖，如果为false，则抛出异常
        if (clazz.isAnnotationPresent(Adaptive.class)) {
            cacheAdaptiveClass(clazz, overridden);
        }

        // 判断当前拓展类是否一个包装类：有一个构造器，且构造器是以当前接口（type）作为入参，也只有这一个入参
        // 如果是包装类，则需要缓存起来
        else if (isWrapperClass(clazz)) {
            cacheWrapperClass(clazz);
        }

        // 其他的处理
        else {

            // 要求类对象有一个无参公开构造器
            //noinspection ResultOfMethodCallIgnored
            clazz.getConstructor();

            // 如果当前SPI拓展类对象没有key，则考虑从类对象上提取
            if (StringUtils.isEmpty(name)) {

                // @Extension注解上提取，如果没有则从类名称上计算提取
                name = findAnnotationName(clazz);

                // @Extension配置的值为空，或clazz的名称和当前接口名称完全一致
                if (name.length() == 0) {
                    throw new IllegalStateException(
                        "No such extension name for the class " + clazz.getName() + " in the config " + resourceURL);
                }
            }

            // 以英文逗号作为标识，对name进行切割
            String[] names = NAME_SEPARATOR.split(name);
            if (ArrayUtils.isNotEmpty(names)) {

                // 缓存被@Activate注解标识的类
                cacheActivateClass(clazz, names[0]);
                for (String n : names) {

                    // 将clazz作为key，和第一个name映射存储
                    cacheName(clazz, n);
                    saveInExtensionClass(extensionClasses, clazz, n, overridden);
                }
            }
        }
    }

    /**
     * cache name
     */
    private void cacheName(Class<?> clazz, String name) {
        if (!cachedNames.containsKey(clazz)) {
            cachedNames.put(clazz, name);
        }
    }

    /**
     * put clazz in extensionClasses
     * <p>
     * 将本次加载的SPI实现类类对象{@code clazz}放入{@code extensionClasses}中
     */
    private void saveInExtensionClass(Map<String, Class<?>> extensionClasses, Class<?> clazz, String name, boolean overridden) {

        // 获取当前name在集合中映射的类对象
        Class<?> c = extensionClasses.get(name);

        // 如果类对象为空，或者可以覆盖，则将本次加载的clazz和name映射放入集合中
        if (c == null || overridden) {
            extensionClasses.put(name, clazz);
        }

        // 否则抛出异常
        else if (c != clazz) {

            // 一个name下有两个实现的这种情况，禁用当前name
            // duplicate implementation is unacceptable
            unacceptableExceptions.add(name);

            // 打印错误日志并抛出异常
            String duplicateMsg =
                "Duplicate extension " + type.getName() + " name " + name + " on " + c.getName() + " and "
                    + clazz.getName();
            logger.error(duplicateMsg);
            throw new IllegalStateException(duplicateMsg);
        }
    }

    /**
     * cache Activate class which is annotated with <code>Activate</code>
     * <p>
     * for compatibility, also cache class with old alibaba Activate annotation
     */
    private void cacheActivateClass(Class<?> clazz, String name) {

        // 如果被Apache的Activate注解标注，则缓存
        Activate activate = clazz.getAnnotation(Activate.class);
        if (activate != null) {
            cachedActivates.put(name, activate);
        }

        // 再判断是否被Alibaba的Activate注解标注，如果有，也缓存
        else {
            // support com.alibaba.dubbo.common.extension.Activate
            com.alibaba.dubbo.common.extension.Activate oldActivate =
                clazz.getAnnotation(com.alibaba.dubbo.common.extension.Activate.class);
            if (oldActivate != null) {
                cachedActivates.put(name, oldActivate);
            }
        }
    }

    /**
     * cache Adaptive class which is annotated with <code>Adaptive</code>
     */
    private void cacheAdaptiveClass(Class<?> clazz, boolean overridden) {

        // 如果当前自适应类对象为空，或者overridden为true（可覆盖），则直接将当前类对象作为自适应类对象
        if (cachedAdaptiveClass == null || overridden) {
            cachedAdaptiveClass = clazz;
        }

        // 如果当前类对象不等于自适应类对象，则抛出异常
        else if (!cachedAdaptiveClass.equals(clazz)) {
            throw new IllegalStateException(
                "More than 1 adaptive class found: " + cachedAdaptiveClass.getName() + ", " + clazz.getName());
        }
    }

    /**
     * cache wrapper class
     * <p>
     * like: ProtocolFilterWrapper, ProtocolListenerWrapper
     */
    private void cacheWrapperClass(Class<?> clazz) {
        if (cachedWrapperClasses == null) {
            cachedWrapperClasses = new ConcurrentHashSet<>();
        }
        cachedWrapperClasses.add(clazz);
    }

    /**
     * test if clazz is a wrapper class
     * <p>
     * which has Constructor with given class type as its only argument
     * <p>
     * 是否包装类：有一个以SPI接口对象为入参的构造器，则为包装类
     */
    private boolean isWrapperClass(Class<?> clazz) {
        try {
            clazz.getConstructor(type);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    @SuppressWarnings("deprecation")
    private String findAnnotationName(Class<?> clazz) {

        // 从@Extionsion注解上提取
        org.apache.dubbo.common.Extension extension = clazz.getAnnotation(org.apache.dubbo.common.Extension.class);
        if (extension != null) {
            return extension.value();
        }

        String name = clazz.getSimpleName();

        // 如果类名称是以当前接口的名称作为结尾，则将类名称上，当前接口名称之前的部分作为key
        if (name.endsWith(type.getSimpleName())) {
            name = name.substring(0, name.length() - type.getSimpleName().length());
        }

        // 否则就将类名称作为key
        return name.toLowerCase();
    }

    /**
     * 创建适配器类
     */
    @SuppressWarnings("unchecked")
    private T createAdaptiveExtension() {
        try {
            return injectExtension((T)getAdaptiveExtensionClass().newInstance());
        } catch (Exception e) {
            throw new IllegalStateException("Can't create adaptive extension " + type + ", cause: " + e.getMessage(),
                e);
        }
    }

    private Class<?> getAdaptiveExtensionClass() {
        getExtensionClasses();
        if (cachedAdaptiveClass != null) {
            return cachedAdaptiveClass;
        }
        return cachedAdaptiveClass = createAdaptiveExtensionClass();
    }

    /**
     * 动态生成适配器类对象
     */
    private Class<?> createAdaptiveExtensionClass() {

        // 生成Adaptive类的代码
        String code = new AdaptiveClassCodeGenerator(type, cachedDefaultName).generate();

        // 获取类加载器
        ClassLoader classLoader = findClassLoader();

        // 获取类编译器
        org.apache.dubbo.common.compiler.Compiler compiler =
            ExtensionLoader.getExtensionLoader(org.apache.dubbo.common.compiler.Compiler.class).getAdaptiveExtension();

        // 编译代码，加载相关的Class并返回
        return compiler.compile(code, classLoader);
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

}
