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

import java.util.concurrent.TimeUnit;

/**
 * 超时的倒计时器
 */
public final class TimeoutCountDown implements Comparable<TimeoutCountDown> {

  /**
   * 初始化一个倒计时器
   *
   * @param timeout 超时时间
   * @param unit    超时单位
   * @return 返回生成的倒计时器
   */
  public static TimeoutCountDown newCountDown(long timeout, TimeUnit unit) {
    return new TimeoutCountDown(timeout, unit);
  }

  /**
   * 超时毫秒数
   */
  private final long timeoutInMillis;

  /**
   * 基于{@link System#nanoTime()}计算出的超时纳秒数
   */
  private final long deadlineInNanos;

  /**
   * 倒计时结束的标识
   */
  private volatile boolean expired;

  private TimeoutCountDown(long timeout, TimeUnit unit) {
    timeoutInMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
    deadlineInNanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
  }

  public long getTimeoutInMilli() {
    return timeoutInMillis;
  }

  public boolean isExpired() {

    // 如果还未过期，则需要判断当前是否过期
    if (!expired) {

      // 如果记录的截止纳秒数小于等于当前系统纳秒数，说明已过期，设置expired标识为true
      if (deadlineInNanos - System.nanoTime() <= 0) {
        expired = true;
      }

      // 否则说明还未过期，直接返回false即可
      else {
        return false;
      }
    }

    // 走到当前链路，说明一定是过期，直接返回true
    return true;
  }

  /**
   * 计算剩余的超时时间
   */
  public long timeRemaining(TimeUnit unit) {

    // 获取当前纳秒数
    final long currentNanos = System.nanoTime();

    // 判断当前是否超时，如果是，则设置expired标识为true
    if (!expired && deadlineInNanos - currentNanos <= 0) {
      expired = true;
    }

    // 计算剩余的超时时间，并转换为对应单位
    return unit.convert(deadlineInNanos - currentNanos, TimeUnit.NANOSECONDS);
  }

  public long elapsedMillis() {
    if (isExpired()) {
      return timeoutInMillis + TimeUnit.MILLISECONDS.convert(System.nanoTime() - deadlineInNanos, TimeUnit.NANOSECONDS);
    } else {
      return timeoutInMillis - TimeUnit.MILLISECONDS.convert(deadlineInNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public String toString() {
    long timeoutMillis = TimeUnit.MILLISECONDS.convert(deadlineInNanos, TimeUnit.NANOSECONDS);
    long remainingMillis = timeRemaining(TimeUnit.MILLISECONDS);

    StringBuilder buf = new StringBuilder();
    buf.append("Total timeout value - ");
    buf.append(timeoutMillis);
    buf.append(", times remaining - ");
    buf.append(remainingMillis);
    return buf.toString();
  }

  @Override
  public int compareTo(TimeoutCountDown another) {
    long delta = this.deadlineInNanos - another.deadlineInNanos;
    if (delta < 0) {
      return -1;
    } else if (delta > 0) {
      return 1;
    }
    return 0;
  }
}
