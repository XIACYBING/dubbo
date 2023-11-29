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
package org.apache.dubbo.config.spring.context;

import com.alibaba.spring.context.OnceApplicationContextEventListener;
import org.apache.dubbo.config.DubboShutdownHook;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;

/**
 * The {@link ApplicationListener} for {@link DubboBootstrap}'s lifecycle when the {@link ContextRefreshedEvent}
 * and {@link ContextClosedEvent} raised
 * <p>
 * 监听Spring容器启动和关闭事件，对{@link DubboBootstrap}的生命周期进行管理
 *
 * @since 2.7.5
 */
public class DubboBootstrapApplicationListener extends OnceApplicationContextEventListener implements Ordered {

    /**
     * The bean name of {@link DubboBootstrapApplicationListener}
     *
     * @since 2.7.6
     */
    public static final String BEAN_NAME = "dubboBootstrapApplicationListener";

    private final DubboBootstrap dubboBootstrap;

    public DubboBootstrapApplicationListener() {
        this.dubboBootstrap = DubboBootstrap.getInstance();
    }

    public DubboBootstrapApplicationListener(ApplicationContext applicationContext) {
        super(applicationContext);

        // 初始化DubboBootstrap
        this.dubboBootstrap = DubboBootstrap.getInstance();

        // 记录Spring应用上下文
        DubboBootstrapStartStopListenerSpringAdapter.applicationContext = applicationContext;
    }

    @Override
    public void onApplicationContextEvent(ApplicationContextEvent event) {
        if (DubboBootstrapStartStopListenerSpringAdapter.applicationContext == null) {
            DubboBootstrapStartStopListenerSpringAdapter.applicationContext = event.getApplicationContext();
        }

        // 启动DubboBootstrap
        if (event instanceof ContextRefreshedEvent) {
            onContextRefreshedEvent((ContextRefreshedEvent)event);
        }

        // 执行Dubbo关闭的钩子
        else if (event instanceof ContextClosedEvent) {
            onContextClosedEvent((ContextClosedEvent)event);
        }
    }

    private void onContextRefreshedEvent(ContextRefreshedEvent event) {
        dubboBootstrap.start();
    }

    private void onContextClosedEvent(ContextClosedEvent event) {
        DubboShutdownHook.getDubboShutdownHook().run();
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
