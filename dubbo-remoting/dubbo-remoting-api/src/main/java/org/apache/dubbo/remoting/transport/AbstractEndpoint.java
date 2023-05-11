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

import org.apache.dubbo.common.Parameters;
import org.apache.dubbo.common.Resetable;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.transport.codec.CodecAdapter;

/**
 * 基于{@link AbstractPeer}的再次抽象实现，并提供{@link Resetable#reset(URL)}的基础实现{@link #reset(URL)}
 * <p>
 * 通过{@link #codec}提供编码解码的相关能力
 * <p>
 * AbstractEndpoint
 */
public abstract class AbstractEndpoint extends AbstractPeer implements Resetable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEndpoint.class);

    /**
     * 基于{@link URL#protocol}获取对应的{@link Codec2}/{@link Codec}实现
     */
    private Codec2 codec;

    /**
     * 根据{@link Constants#CONNECT_TIMEOUT_KEY}从URL上获取超时时间，单位是毫秒
     */
    private int connectTimeout;

    public AbstractEndpoint(URL url, ChannelHandler handler) {
        super(url, handler);
        this.codec = getChannelCodec(url);
        this.connectTimeout =
            url.getPositiveParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT);
    }

    protected static Codec2 getChannelCodec(URL url) {

        // codec 扩展的实现必须和protocol一致
        // codec extension name must stay the same with protocol name
        String codecName = url.getProtocol();

        // 如果扩展存在，则返回对应的扩展
        if (ExtensionLoader.getExtensionLoader(Codec2.class).hasExtension(codecName)) {
            return ExtensionLoader.getExtensionLoader(Codec2.class).getExtension(codecName);
        }

        // 如果扩展不存在，就从即将废弃的Codec扩展中获取需要的实现，并包装成CodecAdapter返回
        else {
            return new CodecAdapter(ExtensionLoader.getExtensionLoader(Codec.class).getExtension(codecName));
        }
    }

    @Override
    public void reset(URL url) {

        // 如果当前端点已关闭，则抛出异常
        if (isClosed()) {
            throw new IllegalStateException(
                "Failed to reset parameters " + url + ", cause: Channel closed. channel: " + getLocalAddress());
        }

        try {

            // 重置超时时间
            if (url.hasParameter(Constants.CONNECT_TIMEOUT_KEY)) {
                int t = url.getParameter(Constants.CONNECT_TIMEOUT_KEY, 0);
                if (t > 0) {
                    this.connectTimeout = t;
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }

        try {

            // 重置Codec2
            if (url.hasParameter(Constants.CODEC_KEY)) {
                this.codec = getChannelCodec(url);
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    @Deprecated
    public void reset(Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    protected Codec2 getCodec() {
        return codec;
    }

    protected int getConnectTimeout() {
        return connectTimeout;
    }

}
