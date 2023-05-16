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

package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.MultiMessage;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.RpcInvocation;

import java.io.IOException;

import static org.apache.dubbo.rpc.Constants.INPUT_KEY;
import static org.apache.dubbo.rpc.Constants.OUTPUT_KEY;

/**
 * {@link DubboCodec}的包装类，提供{@link MultiMessage}的支持能力
 */
public final class DubboCountCodec implements Codec2 {

    private DubboCodec codec = new DubboCodec();

    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        codec.encode(channel, buffer, msg);
    }

    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {

        // 记录当前缓存的读指针
        int save = buffer.readerIndex();

        // 创建一个MultiMessage实例
        MultiMessage result = MultiMessage.create();
        do {

            // 调用DubboCodec.decode，获取缓存中第一份Dubbo协议数据
            Object obj = codec.decode(channel, buffer);

            // 如果被拆包了（如果缓存中的数据量不够一个协议头的长度，或者不够协议头+协议头中记录的payload长度，则认为被拆包了），则重置缓存的读索引，并中断流程
            if (Codec2.DecodeResult.NEED_MORE_INPUT == obj) {
                buffer.readerIndex(save);
                break;
            }

            // 否则将读取出的数据加入到MultiMessage实例中
            else {
                result.addMessage(obj);

                // 记录读取数据量
                logMessageLength(obj, buffer.readerIndex() - save);

                // 更新记录的缓存读索引
                save = buffer.readerIndex();
            }

            // 继续循环，直到剩余数据量不足以组成一个完整的Dubbo协议数据包
        } while (true);

        // 如果读取出的数据为空，则认为数据量不够，被拆包了，返回NEED_MORE_INPUT
        if (result.isEmpty()) {
            return Codec2.DecodeResult.NEED_MORE_INPUT;
        }

        // 如果只有一份数据，则直接返回第一份数据
        if (result.size() == 1) {
            return result.get(0);
        }

        // 否则返回MultiMessage实例
        return result;
    }

    /**
     * 记录消息长度
     */
    private void logMessageLength(Object result, int bytes) {

        // 读取出的数据量为空，不处理
        if (bytes <= 0) {
            return;
        }

        // 根据读取出来的数据类型，记录读取的数据字节数
        if (result instanceof Request) {
            try {
                ((RpcInvocation)((Request)result).getData()).setAttachment(INPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        } else if (result instanceof Response) {
            try {
                ((AppResponse)((Response)result).getResult()).setAttachment(OUTPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        }
    }

}
