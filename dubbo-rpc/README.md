# 说明

`Dubbo`远程调用协议的抽象定义和实现包：

* [dubbo-rpc-api](dubbo-rpc-api)：远程调用协议模块的抽象定义，其他的包都是基于协议对当前抽象定义的实现
* [dubbo-rpc-dubbo](dubbo-rpc-dubbo)：基于`dubbo`协议，对[dubbo-rpc-api](dubbo-rpc-api)进行实现
* [dubbo-rpc-grpc](dubbo-rpc-grpc)：基于`grpc`协议，对[dubbo-rpc-api](dubbo-rpc-api)进行实现
  抽象定义的实现

对于当前包，有以下几个特性；

* 依赖于[dubbo-remoting](../dubbo-remoting)进行远程通信
* 协议实现只关心一对一调用，不关心集群相关实现，集群实现由[dubbo-cluster](../dubbo-cluster)负责