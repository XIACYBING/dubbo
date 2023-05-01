# 说明

`Dubbo`远程通信的抽象定义和实现包：

* [dubbo-remoting-api](dubbo-remoting-api)：远程通信模块的抽象定义，其他的包都是基于第三方开源框架对当前抽象定义的实现
* [dubbo-remoting-netty4](dubbo-remoting-netty4)：基于`netty 4`对[dubbo-remoting-api](dubbo-remoting-api) 抽象定义的实现
* [dubbo-remoting-zookeeper](dubbo-remoting-zookeeper)：基于`Apache Curator`对[dubbo-remoting-api](dubbo-remoting-api)
  抽象定义的实现