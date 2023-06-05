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

import javassist.ClassPool;
import javassist.CtMethod;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * 包装类的类，会将需要包装的类的属性名称（propertyName）和方法名称（methodName）包装起来，然后通过这些数据快速找到要调用的方法并调用
 * <p>
 * 会使用{@code Javassist}生成相应的{@link Wrapper}实现对象
 * <p>
 * Wrapper.
 */
public abstract class Wrapper {

    /**
     * class wrapper map
     * <p>
     * 类包装映射，用于存储类对象和相应的包装类的映射
     */
    private static final Map<Class<?>, Wrapper> WRAPPER_MAP = new ConcurrentHashMap<Class<?>, Wrapper>();
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final String[] OBJECT_METHODS = new String[] {"getClass", "hashCode", "toString", "equals"};
    private static final Wrapper OBJECT_WRAPPER = new Wrapper() {
        @Override
        public String[] getMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getDeclaredMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getPropertyNames() {
            return EMPTY_STRING_ARRAY;
        }

        @Override
        public Class<?> getPropertyType(String pn) {
            return null;
        }

        @Override
        public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public boolean hasProperty(String name) {
            return false;
        }

        @Override
        public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException {
            if ("getClass".equals(mn)) {
                return instance.getClass();
            }
            if ("hashCode".equals(mn)) {
                return instance.hashCode();
            }
            if ("toString".equals(mn)) {
                return instance.toString();
            }
            if ("equals".equals(mn)) {
                if (args.length == 1) {
                    return instance.equals(args[0]);
                }
                throw new IllegalArgumentException("Invoke method [" + mn + "] argument number error.");
            }
            throw new NoSuchMethodException("Method [" + mn + "] not found.");
        }
    };
    private static AtomicLong WRAPPER_CLASS_COUNTER = new AtomicLong(0);

    /**
     * get wrapper.
     *
     * @param c Class instance.
     * @return Wrapper instance(not null).
     */
    public static Wrapper getWrapper(Class<?> c) {

        // can not wrapper on dynamic class.
        // 判断是否实现了DC接口（通过ClassGenerator生成的类都会实现该接口），如果有实现，则无法包装
        // 不断循环，直到获取一个没有实现DC的父类
        while (ClassGenerator.isDynamicClass(c)) {
            c = c.getSuperclass();
        }

        // 如果最终的类对象是Object，则直接返回默认的OBJECT_WRAPPER
        if (c == Object.class) {
            return OBJECT_WRAPPER;
        }

        // 获取类对象对应的包装类
        return WRAPPER_MAP.computeIfAbsent(c, Wrapper::makeWrapper);
    }

    /**
     * 针对类对象生成包装类
     */
    private static Wrapper makeWrapper(Class<?> c) {

        // 基本属性的类对象无法包装
        if (c.isPrimitive()) {
            throw new IllegalArgumentException("Can not create wrapper for primitive type: " + c);
        }

        String name = c.getName();
        ClassLoader cl = ClassUtils.getClassLoader(c);

        // c1中存储最终要生成的setPropertyValue方法代码，以com.xxx.yyy.AService为例，最终生成的代码如下（为了可读性优化过，最终代码会是一行的）：
        // public void setPropertyValue(Object o, String n, Object v){
        //    AService w;
        //    try{
        //         w = ((AService)$1);
        //    }catch(Throwable e){
        //        throw new IllegalArgumentException(e);
        //    }
        //    if( $2.equals("name") ){
        //        w.name=(java.lang.String)$3;
        //        return;
        //    }
        // }
        StringBuilder c1 = new StringBuilder("public void setPropertyValue(Object o, String n, Object v){ ");

        // c2中存储最终要生成的getPropertyValue方法代码，以com.xxx.yyy.AService为例，最终生成的代码如下（为了可读性优化过，最终代码会是一行的）：
        // public Object getPropertyValue(Object o, String n){
        //    AService w;
        //    try{
        //        w = ((AService)$1);
        //    }catch(Throwable e){
        //        throw new IllegalArgumentException(e);
        //    }
        //    if( $2.equals(" if( $2.equals("name") ){
        //        return ($w)w.name;
        //    }
        // }
        StringBuilder c2 = new StringBuilder("public Object getPropertyValue(Object o, String n){ ");

        // c3中存储最终要生成的invokeMethod方法代码，以com.xxx.yyy.AService（内部有sayHello(String s)和sayHelloAsync(String s)两个方法）为例，最终生成的代码如下（为了可读性优化过，最终代码会是一行的）：
        // public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws java.lang.reflect.InvocationTargetException {
        //   com.xxx.yyy.AService w;
        //   try {
        //       w = ((com.xxx.yyy.AService) $1);
        //   } catch (Throwable e) {
        //       throw new IllegalArgumentException(e);
        //   }
        //   try {
        //       // 此处会循环AService下的所有方法（除了继承自Object的方法），并进行生成if代码块进行调用
        //      if ("sayHello".equals($2) && $3.length == 1) {
        //         return ($w) w.sayHello((java.lang.String) $4[0]);
        //      }
        //      if ("sayHelloAsync".equals($2) &amp;&amp; $3.length == 1) {
        //         return ($w) w.sayHelloAsync((java.lang.String) $4[0]);
        //      }
        //   } catch (Throwable e) {
        //     throw new java.lang.reflect.InvocationTargetException(e);
        //   }
        //   throw new NoSuchMethodException("Not found method");
        // }
        StringBuilder c3 = new StringBuilder(
            "public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws "
                + InvocationTargetException.class.getName() + "{ ");

        // 生成获取代理对象的try-catch块代码
        c1
            .append(name)
            .append(" w; try{ w = ((")
            .append(name)
            .append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        c2
            .append(name)
            .append(" w; try{ w = ((")
            .append(name)
            .append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        c3
            .append(name)
            .append(" w; try{ w = ((")
            .append(name)
            .append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");

        // 属性名称和属性类型的映射
        // <property name, property types>
        Map<String, Class<?>> pts = new HashMap<>();

        // 方法描述和Method的映射
        // <method desc, Method instance>
        Map<String, Method> ms = new LinkedHashMap<>();

        // 公共方法名称集合
        // method names.
        List<String> mns = new ArrayList<>();

        // 声明的方法名称集合
        // declaring method names.
        List<String> dmns = new ArrayList<>();

        // 获取所有public的字段
        // get all public field.
        for (Field f : c.getFields()) {
            String fn = f.getName();
            Class<?> ft = f.getType();

            // 不处理静态字段和transient字段
            if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
                continue;
            }

            // 生成get和set的相关方法
            // c1：
            //    if( $2.equals("name") ){
            //        w.name=(java.lang.String)$3;
            //        return;
            //    }
            // c2：
            //    if( $2.equals(" if( $2.equals("name") ){
            //        return ($w)w.name;
            //    }
            c1
                .append(" if( $2.equals(\"")
                .append(fn)
                .append("\") ){ w.")
                .append(fn)
                .append("=")
                .append(arg(ft, "$3"))
                .append("; return; }");
            c2.append(" if( $2.equals(\"").append(fn).append("\") ){ return ($w)w.").append(fn).append("; }");

            // 在集合中添加属性名称和属性的映射
            pts.put(fn, ft);
        }

        // 实例化ClassPool
        final ClassPool classPool = new ClassPool(ClassPool.getDefault());
        classPool.appendClassPath(new CustomizedLoaderClassPath(cl));

        // 获取ClassPool中的所有方法，并生成相关描述到集合中
        List<String> allMethod = new ArrayList<>();
        try {
            final CtMethod[] ctMethods = classPool.get(c.getName()).getMethods();
            for (CtMethod method : ctMethods) {
                allMethod.add(ReflectUtils.getDesc(method));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 获取被代理类的所有public方法，只保留有在allMethod中有数据的方法，并形成集合
        Method[] methods = Arrays.stream(c.getMethods())
                                 .filter(method -> allMethod.contains(ReflectUtils.getDesc(method)))
                                 .collect(Collectors.toList())
                                 .toArray(new Method[] {});
        // get all public method.
        boolean hasMethod = hasMethods(methods);
        if (hasMethod) {
            Map<String, Integer> sameNameMethodCount = new HashMap<>((int) (methods.length / 0.75f) + 1);
            for (Method m : methods) {
                sameNameMethodCount.compute(m.getName(),
                        (key, oldValue) -> oldValue == null ? 1 : oldValue + 1);
            }

            c3.append(" try{");
            for (Method m : methods) {
                //ignore Object's method.
                if (m.getDeclaringClass() == Object.class) {
                    continue;
                }

                String mn = m.getName();
                c3.append(" if( \"").append(mn).append("\".equals( $2 ) ");
                int len = m.getParameterTypes().length;
                c3.append(" && ").append(" $3.length == ").append(len);

                boolean overload = sameNameMethodCount.get(m.getName()) > 1;
                if (overload) {
                    if (len > 0) {
                        for (int l = 0; l < len; l++) {
                            c3.append(" && ").append(" $3[").append(l).append("].getName().equals(\"")
                                    .append(m.getParameterTypes()[l].getName()).append("\")");
                        }
                    }
                }

                c3.append(" ) { ");

                if (m.getReturnType() == Void.TYPE) {
                    c3.append(" w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");").append(" return null;");
                } else {
                    c3.append(" return ($w)w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");");
                }

                c3.append(" }");

                mns.add(mn);
                if (m.getDeclaringClass() == c) {
                    dmns.add(mn);
                }
                ms.put(ReflectUtils.getDesc(m), m);
            }
            c3.append(" } catch(Throwable e) { ");
            c3.append("     throw new java.lang.reflect.InvocationTargetException(e); ");
            c3.append(" }");
        }

        c3.append(" throw new " + NoSuchMethodException.class.getName() + "(\"Not found method \\\"\"+$2+\"\\\" in class " + c.getName() + ".\"); }");

        // deal with get/set method.
        Matcher matcher;
        for (Map.Entry<String, Method> entry : ms.entrySet()) {
            String md = entry.getKey();
            Method method = entry.getValue();
            if ((matcher = ReflectUtils.GETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                String pn = propertyName(matcher.group(1));
                c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
                pts.put(pn, method.getReturnType());
            } else if ((matcher = ReflectUtils.IS_HAS_CAN_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                String pn = propertyName(matcher.group(1));
                c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
                pts.put(pn, method.getReturnType());
            } else if ((matcher = ReflectUtils.SETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                Class<?> pt = method.getParameterTypes()[0];
                String pn = propertyName(matcher.group(1));
                c1
                    .append(" if( $2.equals(\"")
                    .append(pn)
                    .append("\") ){ w.")
                    .append(method.getName())
                    .append("(")
                    .append(arg(pt, "$3"))
                    .append("); return; }");
                pts.put(pn, pt);
            }
        }
        c1.append(" throw new " + NoSuchPropertyException.class.getName()
            + "(\"Not found property \\\"\"+$2+\"\\\" field or setter method in class " + c.getName() + ".\"); }");
        c2.append(" throw new " + NoSuchPropertyException.class.getName()
            + "(\"Not found property \\\"\"+$2+\"\\\" field or getter method in class " + c.getName() + ".\"); }");

        // make class
        long id = WRAPPER_CLASS_COUNTER.getAndIncrement();

        // 生成一个类生成器
        ClassGenerator cc = ClassGenerator.newInstance(cl);

        // 设置类名和父类
        cc.setClassName((Modifier.isPublic(c.getModifiers()) ? Wrapper.class.getName() : c.getName() + "$sw") + id);
        cc.setSuperClass(Wrapper.class);

        // 添加默认构造器
        cc.addDefaultConstructor();

        // 添加静态字段：属性名称数组、属性类型映射、public方法名称数组、所有方法名称数组
        cc.addField("public static String[] pns;"); // property name array.
        cc.addField("public static " + Map.class.getName() + " pts;"); // property type map.
        cc.addField("public static String[] mns;"); // all method name array.
        cc.addField("public static String[] dmns;"); // declared method name array.

        // 按照ms数组的顺序，添加方法参数类型数组：第一个方法的参数类型数组名称是：mts1
        for (int i = 0, len = ms.size(); i < len; i++) {
            cc.addField("public static Class[] mts" + i + ";");
        }

        // 添加getPropertyNames、hasProperty、getPropertyType、getMethodNames和getDeclaredMethodNames方法
        cc.addMethod("public String[] getPropertyNames(){ return pns; }");
        cc.addMethod("public boolean hasProperty(String n){ return pts.containsKey($1); }");
        cc.addMethod("public Class getPropertyType(String n){ return (Class)pts.get($1); }");
        cc.addMethod("public String[] getMethodNames(){ return mns; }");
        cc.addMethod("public String[] getDeclaredMethodNames(){ return dmns; }");

        // 添加get字段、set字段和invokeMethod的通用代码
        cc.addMethod(c1.toString());
        cc.addMethod(c2.toString());
        cc.addMethod(c3.toString());

        try {

            // 生成Wrapper子类的类对象
            Class<?> wc = cc.toClass();

            // 设置相关静态字段的值
            // setup static field.
            wc.getField("pts").set(null, pts);
            wc.getField("pns").set(null, pts.keySet().toArray(new String[0]));
            wc.getField("mns").set(null, mns.toArray(new String[0]));
            wc.getField("dmns").set(null, dmns.toArray(new String[0]));
            int ix = 0;

            // 设置参数类型值
            for (Method m : ms.values()) {
                wc.getField("mts" + ix++).set(null, m.getParameterTypes());
            }

            // 生成Wrapper实例
            return (Wrapper)wc.getDeclaredConstructor().newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            cc.release();
            ms.clear();
            mns.clear();
            dmns.clear();
        }
    }

    private static String arg(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (cl == Boolean.TYPE) {
                return "((Boolean)" + name + ").booleanValue()";
            }
            if (cl == Byte.TYPE) {
                return "((Byte)" + name + ").byteValue()";
            }
            if (cl == Character.TYPE) {
                return "((Character)" + name + ").charValue()";
            }
            if (cl == Double.TYPE) {
                return "((Number)" + name + ").doubleValue()";
            }
            if (cl == Float.TYPE) {
                return "((Number)" + name + ").floatValue()";
            }
            if (cl == Integer.TYPE) {
                return "((Number)" + name + ").intValue()";
            }
            if (cl == Long.TYPE) {
                return "((Number)" + name + ").longValue()";
            }
            if (cl == Short.TYPE) {
                return "((Number)" + name + ").shortValue()";
            }
            throw new RuntimeException("Unknown primitive type: " + cl.getName());
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    private static String args(Class<?>[] cs, String name) {
        int len = cs.length;
        if (len == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(arg(cs[i], name + "[" + i + "]"));
        }
        return sb.toString();
    }

    private static String propertyName(String pn) {
        return pn.length() == 1 || Character.isLowerCase(pn.charAt(1)) ? Character.toLowerCase(pn.charAt(0)) + pn.substring(1) : pn;
    }

    private static boolean hasMethods(Method[] methods) {
        if (methods == null || methods.length == 0) {
            return false;
        }
        for (Method m : methods) {
            if (m.getDeclaringClass() != Object.class) {
                return true;
            }
        }
        return false;
    }

    /**
     * get property name array.
     *
     * @return property name array.
     */
    abstract public String[] getPropertyNames();

    /**
     * get property type.
     *
     * @param pn property name.
     * @return Property type or nul.
     */
    abstract public Class<?> getPropertyType(String pn);

    /**
     * has property.
     *
     * @param name property name.
     * @return has or has not.
     */
    abstract public boolean hasProperty(String name);

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @return value.
     */
    abstract public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @param pv       property value.
     */
    abstract public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @return value array.
     */
    public Object[] getPropertyValues(Object instance, String[] pns) throws NoSuchPropertyException, IllegalArgumentException {
        Object[] ret = new Object[pns.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = getPropertyValue(instance, pns[i]);
        }
        return ret;
    }

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @param pvs      property value array.
     */
    public void setPropertyValues(Object instance, String[] pns, Object[] pvs) throws NoSuchPropertyException, IllegalArgumentException {
        if (pns.length != pvs.length) {
            throw new IllegalArgumentException("pns.length != pvs.length");
        }

        for (int i = 0; i < pns.length; i++) {
            setPropertyValue(instance, pns[i], pvs[i]);
        }
    }

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getMethodNames();

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getDeclaredMethodNames();

    /**
     * has method.
     *
     * @param name method name.
     * @return has or has not.
     */
    public boolean hasMethod(String name) {
        for (String mn : getMethodNames()) {
            if (mn.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * invoke method.
     *
     * @param instance instance.
     * @param mn       method name.
     * @param types
     * @param args     argument array.
     * @return return value.
     */
    abstract public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException, InvocationTargetException;
}
