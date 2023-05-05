/**
 * 网络传输层的抽象，但是只负责抽象单项消息的传输：请求由Client发出，Server接收，响应由Server发出，Client传输；
 * <p>
 * 实际的情况由具体框架进行实现
 *
 * @author wang.yubin
 * @since 2023/5/5
 */
package org.apache.dubbo.remoting.transport;