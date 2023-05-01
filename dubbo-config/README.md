# 说明

`Dubbo`配置的抽象定义和实现包，`Dubbo`对外暴露的配置都是由当前模块定义、实现和解析的：

* [dubbo-config-api](dubbo-config-api)：以`api`方式使用`Dubbo`时，相关配置所在的模块
* [dubbo-config-default](dubbo-config-spring)：继承`Spring`框架使用`Dubbo`时，相关配置所在的模块