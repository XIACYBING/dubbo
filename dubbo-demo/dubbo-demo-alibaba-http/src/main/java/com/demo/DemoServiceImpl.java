package com.demo;

import com.alibaba.dubbo.config.annotation.Service;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.demo.DemoService;

/**
 * @author wang.yubin
 * @since 2023/6/8
 */
@Slf4j
@Service
public class DemoServiceImpl implements DemoService {

    @Override
    public String sayHello(String name) {
        log.info("hello [{}]", name);
        return "hello " + name;
    }

}
