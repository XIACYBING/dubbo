# 说明

`Dubbo`注册中心的抽象定义和实现包：

* [dubbo-registry-api](dubbo-registry-api)：注册中心模块的抽象定义，其他的包都是基于第三方开源框架对当前抽象定义的实现
* [dubbo-registry-zookeeper](dubbo-registry-zookeeper)：基于`Zookeeper`对[dubbo-registry-api](dubbo-registry-api)抽象定义的实现