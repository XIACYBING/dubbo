package com.demo.rpc;

import com.alibaba.fastjson.JSON;
import com.demo.EmbeddedZooKeeper;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.demo.DemoService;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * apache版本的跨语言调用样例，alibaba版本的跨语言调用样例在{@code dubbo-demo/dubbo-demo-alibaba-http}下的{@link com.demo.Provider}中
 *
 * @author wang.yubin
 * @since 2023/6/8
 */
@Slf4j
public class Provider {

    private static final int ZK_PORT = ThreadLocalRandom.current().nextInt(1 << 16);
    private static final int HTTP_PORT = 1501;

    public static void main(String[] args) {

        EmbeddedZooKeeper zk = EmbeddedZooKeeper.start(ZK_PORT, false);

        try {

            DubboBootstrap bootstrap = DubboBootstrap.getInstance();

            ProtocolConfig protocolConfig = new ProtocolConfig("http", HTTP_PORT);
            protocolConfig.setServer("tomcat");

            bootstrap
                .application("provider")
                .registry(new RegistryConfig("zookeeper://127.0.0.1:" + ZK_PORT, "http"))
                .protocol(protocolConfig);

            // 初始化serviceConfig
            ServiceConfig<DemoService> serviceConfig = new ServiceConfig<>();
            serviceConfig.setInterface(DemoService.class);
            serviceConfig.setRef(new DemoServiceImpl());

            bootstrap.service(serviceConfig).start();

            log.info("Provider启动完成");

            bootstrap.await();

        } finally {
            DubboBootstrap.getInstance().stop();

            zk.stop();
        }
    }

    static class Consumer {
        public static void main(String[] args) throws IOException, NoSuchMethodException {

            try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

                // 初始化Post请求
                HttpPost post = new HttpPost("http://127.0.0.1:" + HTTP_PORT + "/" + DemoService.class.getName());

                // 获取要调用的方法
                Method method = DemoService.class.getDeclaredMethod("sayHello", String.class);

                // 初始化请求参数
                Map<String, Object> paramMap = new HashMap<>(3, 1);
                paramMap.put("id", 1);
                paramMap.put("method", method.getName());
                paramMap.put("params", new Object[] {"apache http rpc"});

                // 设置请求数据到Post请求中
                post.setEntity(new StringEntity(JSON.toJSONString(paramMap), ContentType.APPLICATION_JSON));

                // 发起请求
                CloseableHttpResponse response = client.execute(post);

                log.info("Http请求结果：[{}]", response);

                // 读取请求数据并输出
                log.info("响应数据：[{}]", EntityUtils.toString(response.getEntity()));
            }
        }
    }

}
