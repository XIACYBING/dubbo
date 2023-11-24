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
package org.apache.dubbo.rpc.cluster.router;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.governance.GovernanceRuleRepository;

/**
 * 抽象路由规则
 */
public abstract class AbstractRouter implements Router {

    /**
     * 路由规则优先级
     */
    protected int priority = DEFAULT_PRIORITY;

    /**
     * 当路由结果为空时，是否强制执行，如果不强制执行，路由结果为空的路由规则将自动失效。该字段默认值为 false。
     * <p>
     * false：路由结果为空时，失效空结果
     * <p>
     * 当前该字段无用
     */
    protected boolean force = false;

    /**
     * 路由规则相关url
     */
    protected URL url;

    protected GovernanceRuleRepository ruleRepository;

    public AbstractRouter(URL url) {
        this.ruleRepository = ExtensionLoader.getExtensionLoader(GovernanceRuleRepository.class).getDefaultExtension();
        this.url = url;
    }

    public AbstractRouter() {
    }

    @Override
    public URL getUrl() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public boolean isRuntime() {
        return true;
    }

    @Override
    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

}
