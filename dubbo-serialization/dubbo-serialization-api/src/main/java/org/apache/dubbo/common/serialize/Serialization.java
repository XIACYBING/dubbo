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
package org.apache.dubbo.common.serialize;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 序列化接口，默认序列化实现为{@link org.apache.dubbo.common.serialize.hessian2.Hessian2Serialization}
 * <p>
 * Serialization strategy interface that specifies a serializer. (SPI, Singleton, ThreadSafe)
 * <p>
 * The default extension is hessian2 and the default serialization implementation of the dubbo protocol.
 * <pre>
 *     e.g. &lt;dubbo:protocol serialization="xxx" /&gt;
 * </pre>
 */
@SPI("hessian2")
public interface Serialization {

    /**
     * 每种序列化算法都对应着一种ContentTypeId，相关的ContentType都定义在{@link Constants}
     * <p>
     * Get content type unique id, recommended that custom implementations use values different with
     * any value of {@link Constants} and don't greater than ExchangeCodec.SERIALIZATION_MASK (31)
     * because dubbo protocol use 5 bits to record serialization ID in header.
     *
     * @return content type id
     */
    byte getContentTypeId();

    /**
     * 序列化ContentType的字符串描述，由每个{@link Serialization}实现类自行维护
     * <p>
     * Get content type
     *
     * @return content type
     */
    String getContentType();

    /**
     * 获取一个{@link ObjectOutput}对象，负责实现序列化功能，即将Java对象转化为字节序列
     * <p>
     * Get a serialization implementation instance
     *
     * @param url    URL address for the remote service
     * @param output the underlying output stream
     * @return serializer
     * @throws IOException
     */
    @Adaptive
    ObjectOutput serialize(URL url, OutputStream output) throws IOException;

    /**
     * 获取一个{@link ObjectInput}对象，负责实现反序列化功能，即将字节序列转化为Java对象
     * <p>
     * Get a deserialization implementation instance
     *
     * @param url   URL address for the remote service
     * @param input the underlying input stream
     * @return deserializer
     * @throws IOException
     */
    @Adaptive
    ObjectInput deserialize(URL url, InputStream input) throws IOException;

}
