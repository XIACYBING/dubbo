package org.apache.dubbo.config;

import com.demo.EmbeddedZooKeeper;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.utils.ReferenceConfigCache;
import org.apache.dubbo.demo.DemoService;

/**
 * 异步生成代理的Dubbo代码示例
 * <p>
 * 3.0之后，Dubbo支持异步的{@code export}和{@code refer}操作，可以通过{@link DubboBootstrap#exportAsync()}和
 * {@link DubboBootstrap#referAsync()}达成目的
 * <p>
 * todo dubbo官网目前没有关于这方面的说明，后面需要补充
 *
 * @author wang.yubin
 * @see <a href="https://developer.aliyun.com/article/1225078">带你读《Apache Dubbo微服务开发从入门到精通》——六、 配置项手册（9）</a>
 * @since 2023/6/8
 */
@Slf4j
public class Provider {

    private static final int ZK_PORT = 1500;
    private static final int DUBBO_PORT = 1501;

    public static void main(String[] args) {

        EmbeddedZooKeeper zk = EmbeddedZooKeeper.start(ZK_PORT, false);

        try {

            DubboBootstrap bootstrap = DubboBootstrap.getInstance();

            ProtocolConfig protocolConfig = new ProtocolConfig("dubbo", DUBBO_PORT);

            bootstrap
                .application("provider")
                .registry(new RegistryConfig("zookeeper://127.0.0.1:" + ZK_PORT, "dubbp"))
                .protocol(protocolConfig);

            // 初始化serviceConfig
            ServiceConfig<DemoService> serviceConfig = new ServiceConfig<>();
            serviceConfig.setInterface(DemoService.class);
            serviceConfig.setRef(name -> name);

            bootstrap.service(serviceConfig).start();

            log.info("Provider启动完成");

            bootstrap.await();
        } finally {
            DubboBootstrap.getInstance().stop();

            zk.stop();
        }
    }

    static class Consumer {
        public static void main(String[] args) {

            try {

                DubboBootstrap bootstrap = DubboBootstrap.getInstance();

                ProtocolConfig protocolConfig = new ProtocolConfig("dubbo", DUBBO_PORT);

                // 初始化serviceConfig
                ReferenceConfig<DemoService> referenceConfig = new ReferenceConfig<>();
                referenceConfig.setInterface(DemoService.class);

                bootstrap
                    .application("consumer")
                    .registry(new RegistryConfig("zookeeper://127.0.0.1:" + ZK_PORT, "http"))
                    .protocol(protocolConfig)
                    .reference(referenceConfig)
                    .start();

                log.info("Consumer启动完成");

                DemoService demoService = ReferenceConfigCache.getCache().get(referenceConfig);
                log.info("调用provider：[{}]", demoService.sayHello("hello"));

                bootstrap.await();
            } finally {
                DubboBootstrap.getInstance().stop();
            }
        }
    }

}
