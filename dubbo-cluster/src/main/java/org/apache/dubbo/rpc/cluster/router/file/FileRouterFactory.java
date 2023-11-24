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
package org.apache.dubbo.rpc.cluster.router.file;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.utils.IOUtils;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.router.script.ScriptRouterFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RULE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RUNTIME_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.TYPE_KEY;

/**
 * {@link ScriptRouterFactory}的装饰器，一般由{@link FileRouterFactory}负责读取文件内容，由{@link ScriptRouterFactory}将文件内容转换成路由规则
 */
public class FileRouterFactory implements RouterFactory {

    public static final String NAME = "file";

    /**
     * 实际进行路由规则转换的路由工厂，一般是{@link ScriptRouterFactory}
     */
    private RouterFactory routerFactory;

    public void setRouterFactory(RouterFactory routerFactory) {
        this.routerFactory = routerFactory;
    }

    @Override
    public Router getRouter(URL url) {
        try {

            // 加载文件内容，并转换成script协议开头的url
            // Transform File URL into Script Route URL, and Load
            // file:///d:/path/to/route.js?router=script ==> script:///d:/path/to/route.js?type=js&rule=<file-content>

            // 获取路由规则协议，默认为script
            // Replace original protocol (maybe 'file') with 'script'
            String protocol = url.getParameter(ROUTER_KEY, ScriptRouterFactory.NAME);
            // Use file suffix to config script type, e.g., js, groovy ...
            String type = null;
            String path = url.getPath();

            // 如果有后缀，则以文件后缀作为type
            if (path != null) {
                int i = path.lastIndexOf('.');
                if (i > 0) {
                    type = path.substring(i + 1);
                }
            }

            // 读取文件内容
            String rule = IOUtils.read(new FileReader(new File(url.getAbsolutePath())));

            // FIXME: this code looks useless
            boolean runtime = url.getParameter(RUNTIME_KEY, false);
            URL script = URLBuilder
                .from(url)
                .setProtocol(protocol)
                .addParameter(TYPE_KEY, type)
                .addParameter(RUNTIME_KEY, runtime)
                .addParameterAndEncoded(RULE_KEY, rule)
                .build();

            // 使用内置的实际路由工厂生成路由，实际上就是ScriptRouterFactory
            return routerFactory.getRouter(script);
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
