package com.demo;

import com.alibaba.dubbo.container.Container;
import com.alibaba.dubbo.container.javaconfig.JavaConfigContainer;
import com.alibaba.dubbo.rpc.protocol.http.HttpProtocol;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.demo.DemoService;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter;
import org.springframework.remoting.support.RemoteInvocation;
import org.springframework.remoting.support.RemoteInvocationResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.dubbo.container.javaconfig.JavaConfigContainer.SPRING_JAVACONFIG;

/**
 * alibaba版本的Dubbo和apache版本的Dubbo，在跨语言调用的{@link HttpProtocol}的请求处理器{@link HttpProtocol.InternalHandler}
 * 实现上不太一致，因此此处记录alibaba版本的Dubbo的跨语言调用样例
 * <p>
 * alibaba版本的跨语言调用基于{@code Spring rmi}中的{@link HttpInvokerServiceExporter}，貌似只能对对象二进制流进行处理；
 * apache版本的跨语言调用则是基于{@link com.googlecode.jsonrpc4j.JsonRpcServer}，可以处理Json字符串，实用性更高；
 * <p>
 * apache版本的跨语言调用样例在{@code dubbo-demo/dubbo-demo-http-json-rpc}下的{@link com.demo.rpc.Provider}中
 * <p>
 * alibaba版本的跨语言调用说实话实用性不高，因为数据需要基于对象序列化为字节后进行传输，才能被{@link HttpInvokerServiceExporter}解析成{@link RemoteInvocation}
 * ，进而完成请求，接收方也要对对象反序列化后才能获取到真正的返回数据
 *
 * @author wang.yubin
 * @since 2023/6/8
 */
@Slf4j
public class Provider {

    public static final int ZK_PORT = ThreadLocalRandom.current().nextInt(1 << 16);
    public static final int HTTP_PORT = 1501;

    public static void main(String[] args) throws InterruptedException {

        EmbeddedZooKeeper zk = EmbeddedZooKeeper.start(ZK_PORT, false);

        Container container = new JavaConfigContainer();

        try {
            System.setProperty(SPRING_JAVACONFIG, Provider.class.getPackage().getName());

            container.start();

            log.info("Provider启动完成");

            synchronized (Container.class) {
                Container.class.wait();
            }

        } finally {
            container.stop();
            zk.stop();
        }
    }

    static class Consumer {
        public static void main(String[] args) throws IOException, NoSuchMethodException, ClassNotFoundException {

            try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

                // 初始化Post请求
                HttpPost post = new HttpPost("http://127.0.0.1:" + HTTP_PORT + "/" + DemoService.class.getName());

                // 获取要调用的方法
                Method method = DemoService.class.getDeclaredMethod("sayHello", String.class);

                // 初始化RemoteInvocation
                RemoteInvocation invocation = new RemoteInvocation();
                invocation.setMethodName(method.getName());
                invocation.setParameterTypes(method.getParameterTypes());
                invocation.setArguments(new Object[] {"consumer"});

                // 生成对象的数据流
                ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
                objectOutputStream.writeObject(invocation);

                // 初始化Http请求数据
                BasicHttpEntity entity = new BasicHttpEntity();
                entity.setContent(new ByteArrayInputStream(byteOutputStream.toByteArray()));

                // 设置请求数据到Post请求中
                post.setEntity(entity);

                // 发起请求
                CloseableHttpResponse response = client.execute(post);

                log.info("Http请求结果：[{}]", response);

                // 读取请求数据
                RemoteInvocationResult result =
                    (RemoteInvocationResult)new ObjectInputStream(response.getEntity().getContent()).readObject();

                log.info("响应数据：[{}]", JSON.toJSONString(result));
            }
        }
    }

}
