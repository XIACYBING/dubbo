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
package org.apache.dubbo.remoting;

import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;

import java.io.IOException;

/**
 * 编解码接口：可实现字节数据和有意义消息之间的转换，或消息之间的互相转换
 * <p>
 * 会根据{@link org.apache.dubbo.common.URL}上的{@link Constants#CODEC_KEY}参数决定实际使用的{@link Codec2}实现类
 */
@SPI
public interface Codec2 {

    @Adaptive( {Constants.CODEC_KEY})
    void encode(Channel channel, ChannelBuffer buffer, Object message) throws IOException;

    @Adaptive( {Constants.CODEC_KEY})
    Object decode(Channel channel, ChannelBuffer buffer) throws IOException;

    /**
     * 处理TCP传输时粘包和拆包的枚举
     */
    enum DecodeResult {

        /**
         * 当前读取到的数据不足以构成一个消息时，需要更多的数据
         */
        NEED_MORE_INPUT,
        SKIP_SOME_INPUT
    }

}

