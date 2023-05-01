# 说明

`Dubbo`中负责集群管理的模块：

* 提供了负载均衡、容错和路由等一系列集群相关功能
* 最终目的是将多个Provider伪装成一个Provider，让Consumer能像调用一个Provider一样调用Provider集群