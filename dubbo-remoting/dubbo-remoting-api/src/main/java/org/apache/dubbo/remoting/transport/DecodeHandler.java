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

package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.Decodeable;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;

/**
 * 解码处理器，并非使用{@link Codec2}进行解码，而是如果{@code message}/{@link Request#mData}/{@link Response#mResult}继承了
 * {@link Decodeable}，则对{@link Decodeable#decode()}方法进行调用
 * <p>
 * {@link DecodeHandler}是对请求体{@link Request#mData}和响应结果{@link Response#mResult}进行解码，而{@link Codec2}是对整个
 * {@link Request}和{@link Response}进行编码和解码（会根据配置判断是否需要对请求体和相应结果进行解码，如果不需要，则会由{@link DecodeHandler}承担解码工作）
 * <p>
 * 在{@link org.apache.dubbo.rpc.protocol.dubbo.DubboCodec#decodeBody}的解码操作中，会根据url
 * 上配置的{@link org.apache.dubbo.rpc.protocol.dubbo.Constants#DEFAULT_DECODE_IN_IO_THREAD}属性，判断是否在IO线程中进行解码（比如Netty的IO线程）
 */
public class DecodeHandler extends AbstractChannelHandlerDelegate {

    private static final Logger log = LoggerFactory.getLogger(DecodeHandler.class);

    public DecodeHandler(ChannelHandler handler) {
        super(handler);
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {

        // message本身可解码
        if (message instanceof Decodeable) {
            decode(message);
        }

        // 对Request中的data进行解码
        if (message instanceof Request) {
            decode(((Request)message).getData());
        }

        // 对Response中的result进行解码
        if (message instanceof Response) {
            decode(((Response)message).getResult());
        }

        // 继续进行消息接收处理
        handler.received(channel, message);
    }

    private void decode(Object message) {

        // 如果message可解码，则调用它本身的解码方法
        if (message instanceof Decodeable) {
            try {
                ((Decodeable)message).decode();
                if (log.isDebugEnabled()) {
                    log.debug("Decode decodeable message " + message.getClass().getName());
                }
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn("Call Decodeable.decode failed: " + e.getMessage(), e);
                }
            } // ~ end of catch
        } // ~ end of if
    } // ~ end of method decode

}
