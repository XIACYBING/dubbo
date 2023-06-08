package com.demo;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ProviderConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.demo.DemoService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.demo.Provider.HTTP_PORT;
import static com.demo.Provider.ZK_PORT;

/**
 * @author wang.yubin
 * @since 2023/6/8
 */
@Configuration
@SuppressWarnings("SpringFacetCodeInspection")
public class DubboConfig {

    @Bean
    public ApplicationConfig applicationConfig() {
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName("alibaba-dubbo");
        return applicationConfig;
    }

    @Bean
    public ProviderConfig providerConfig() {
        ProviderConfig providerConfig = new ProviderConfig();
        providerConfig.setDefault(Boolean.TRUE);
        providerConfig.setThreads(1);
        providerConfig.setExecutes(1);
        return providerConfig;
    }

    @Bean
    public ProtocolConfig protocolConfig() {
        // todo 端口写入到zk
        ProtocolConfig protocolConfig = new ProtocolConfig("http", HTTP_PORT);
        protocolConfig.setServer("tomcat");
        return protocolConfig;
    }

    @Bean
    public RegistryConfig registryConfig() {
        RegistryConfig registryConfig = new RegistryConfig("zookeeper://127.0.0.1:" + ZK_PORT);
        registryConfig.setProtocol("http");
        return registryConfig;
    }

    @Bean
    public DemoServiceImpl demoService() {
        return new DemoServiceImpl();
    }

    /**
     * 由{@link ServiceBean#afterPropertiesSet()}触发Dubbo流程的相关初始化
     */
    @Bean
    public ServiceBean<DemoService> serviceBean(DemoServiceImpl demoService) {
        ServiceBean<DemoService> serviceBean = new ServiceBean<>(DemoServiceImpl.class.getAnnotation(Service.class));
        serviceBean.setInterface(DemoService.class);
        serviceBean.setRef(demoService);
        return serviceBean;
    }

}
