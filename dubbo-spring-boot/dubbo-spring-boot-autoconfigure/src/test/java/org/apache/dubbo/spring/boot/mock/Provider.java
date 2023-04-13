package org.apache.dubbo.spring.boot.mock;

import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.spring.boot.mock.Provider.MockService;
import static org.apache.dubbo.spring.boot.mock.Provider.ZK_PORT;

/**
 * 提供者
 *
 * @author wang.yubin
 * @since 2023/4/13
 */
@Slf4j
public class Provider {

    public static final int ZK_PORT = 2181;

    public static void main(String[] args) {

        EmbeddedZooKeeper zk = new EmbeddedZooKeeper(ZK_PORT, false);
        try {
            zk.start();

            DubboBootstrap bootstrap = DubboBootstrap.getInstance();
            bootstrap.application("provider")
                     .registry(new RegistryConfig("zookeeper://127.0.0.1:" + ZK_PORT, "dubbo"))
                     .protocol(new ProtocolConfig("dubbo", 38091));

            // 初始化serviceConfig
            ServiceConfig<MockService> serviceConfig = new ServiceConfig<>();
            serviceConfig.setInterface(MockService.class);
            serviceConfig.setRef(new MockServiceImpl());

            bootstrap.service(serviceConfig).start();

            log.info("Provider启动完成");

            bootstrap.await();


        } finally {
            DubboBootstrap.getInstance().stop();
            zk.stop();
        }
    }

    interface MockService {

        String mockStr(String mockStr);

        String toString();

    }

    @Slf4j
    static class MockServiceImpl implements MockService {

        @Override
        public String mockStr(String mockStr) {
            log.info("接收到mockStr：[{}]", mockStr);
            return mockStr + 1;
        }

        @Override
        public String toString() {
            log.info("接收到调用：org.apache.dubbo.spring.boot.mock.Provider.MockServiceImpl.toString");
            return "super.toString()";
        }
    }
    
}

@Slf4j
class Consumer {

    public static void main(String[] args) {

        try {
            DubboBootstrap bootstrap = DubboBootstrap
                .getInstance()
                .application("consumer")
                .registry(new RegistryConfig("zookeeper://127.0.0.1:" + ZK_PORT, "dubbo"))
                .protocol(new ProtocolConfig("dubbo", 38090));

            ReferenceConfig<MockService> referenceConfig = new ReferenceConfig<>();
            referenceConfig.setInterface(MockService.class);
            referenceConfig.setUrl("dubbo://" + NetUtils.getLocalAddress().getHostAddress() + ":38091");

            // 需要debug，尽量不超时
            referenceConfig.setTimeout(Integer.MAX_VALUE);
            bootstrap.reference(referenceConfig);
            bootstrap.start();

            log.info("consumer启动完成");

            String str = referenceConfig.get().mockStr(String.valueOf(ThreadLocalRandom.current().nextLong()));
            System.out.println("mockStr：" + str);

            System.out.println("toString：" + referenceConfig.get().toString());
        } finally {
            DubboBootstrap.getInstance().stop();
        }
    }

}
