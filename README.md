# 目的

这是一个学习项目，参考 rocketmq 造的轮子



# 模块

### router

注册中心，类比 rocketmq 的 namesrv



### broker

服务端，也是数据存放的地方，类比 rocketmq 的 broker



### client

客户端，消费者和生产者属于该模块



### common

模块间公用的工具类和实体类



### test

测试模块



# 实现的功能



支持生产者同步或异步生产消息，消费者异步消费消息

支持消息不丢失

支持 broker 集群自动选举，数据同步

支持 broker 多主多从

支持消息分片存储

支持 consumer 集群消费



# 其他



Java9 及以上的版本需要配置 JVM 参数 --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED



