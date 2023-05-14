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
package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.exchange.support.header.HeaderExchangeHandler;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.HEARTBEAT_EVENT;

/**
 * Request.
 * <p>
 * 对于接收到请求的一端，解码请求的位置是在对应的{@link Codec2}实现中
 */
public class Request {

    /**
     * 静态常量字段：用于生成请求的自增id，递增到{@link Long#MAX_VALUE}后，会移除到{@link Long#MIN_VALUE}，让我们可以继续使用
     */
    private static final AtomicLong INVOKE_ID = new AtomicLong(0);

    /**
     * 当前请求的id
     */
    private final long mId;

    /**
     * 当前请求的请求协议版本号：{@link Version#getProtocolVersion()}
     */
    private String mVersion;

    /**
     * 请求双向标识，如果为true，则代表Server在接收到请求后，需要给Client一个响应，false则不需要响应
     */
    private boolean mTwoWay = true;

    /**
     * 事件标识：比如心跳请求/只读请求等，该标识会为true
     */
    private boolean mEvent = false;

    /**
     * 解码异常的标识：Server接收到请求的二进制数据后，由{@link Codec2}将二进制数据解码成{@link Request}对象，如果解码环节遇到异常，则会设置当前标识为true
     * ，并将具体的异常设置到{@link #mData}中然后让其他的{@link ChannelHandler}对该情况做进一步的处理（一般是由{@link HeaderExchangeHandler}
     * 直接设置BAD_REQUEST响应，并返回）
     */
    private boolean mBroken = false;

    /**
     * 请求的具体数据，也可能是解码时的异常
     * <p>
     * 类型一般是{@link org.apache.dubbo.rpc.RpcInvocation}
     */
    private Object mData;

    public Request() {
        mId = newId();
    }

    public Request(long id) {
        mId = id;
    }

    private static long newId() {
        // getAndIncrement() When it grows to MAX_VALUE, it will grow to MIN_VALUE, and the negative can be used as ID
        return INVOKE_ID.getAndIncrement();
    }

    private static String safeToString(Object data) {
        if (data == null) {
            return null;
        }
        String dataStr;
        try {
            dataStr = data.toString();
        } catch (Throwable e) {
            dataStr = "<Fail toString of " + data.getClass() + ", cause: " +
                    StringUtils.toString(e) + ">";
        }
        return dataStr;
    }

    public long getId() {
        return mId;
    }

    public String getVersion() {
        return mVersion;
    }

    public void setVersion(String version) {
        mVersion = version;
    }

    public boolean isTwoWay() {
        return mTwoWay;
    }

    public void setTwoWay(boolean twoWay) {
        mTwoWay = twoWay;
    }

    public boolean isEvent() {
        return mEvent;
    }

    public void setEvent(String event) {
        this.mEvent = true;
        this.mData = event;
    }

    public void setEvent(boolean mEvent) {
        this.mEvent = mEvent;
    }

    public boolean isBroken() {
        return mBroken;
    }

    public void setBroken(boolean mBroken) {
        this.mBroken = mBroken;
    }

    public Object getData() {
        return mData;
    }

    public void setData(Object msg) {
        mData = msg;
    }

    public boolean isHeartbeat() {
        return mEvent && HEARTBEAT_EVENT == mData;
    }

    public void setHeartbeat(boolean isHeartbeat) {
        if (isHeartbeat) {
            setEvent(HEARTBEAT_EVENT);
        }
    }

    public Request copy() {
        Request copy = new Request(mId);
        copy.mVersion = this.mVersion;
        copy.mTwoWay = this.mTwoWay;
        copy.mEvent = this.mEvent;
        copy.mBroken = this.mBroken;
        copy.mData = this.mData;
        return copy;
    }

    @Override
    public String toString() {
        return "Request [id=" + mId + ", version=" + mVersion + ", twoway=" + mTwoWay + ", event=" + mEvent
                + ", broken=" + mBroken + ", data=" + (mData == this ? "this" : safeToString(mData)) + "]";
    }
}
