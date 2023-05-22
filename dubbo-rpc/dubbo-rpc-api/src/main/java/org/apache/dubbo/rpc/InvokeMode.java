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

import org.apache.dubbo.remoting.exchange.support.DefaultFuture;

import java.util.concurrent.Future;

public enum InvokeMode {

    /**
     * 同步调用
     */
    SYNC,

    /**
     * 异步调用
     */
    ASYNC,

    /**
     * {@link Future}调用，会将承载结果的{@link DefaultFuture}直接返回给外部
     */
    FUTURE;

}
