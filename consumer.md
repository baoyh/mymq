### 流程

------

1. Consumer 配置 Router 地址，要订阅的 Topic，所在的 Group

2. Consumer 配置消费接口 MessageListener 接收消息

3. Consumer 启动

   3.1 根据 Router 地址找到所有的 Broker，通过负载均衡算法选出一个 Broker 和 Queue

   3.2 将 Topic 、Group、Queue 发送给选出的 Broker 进行消费

   3.3 Broker 端存放了 <topic@group, <queueId, offset >> consumerOffsetTable 用于存放 Consumer 已经消费过的最新偏移和 <topic,<queueId, offset >> offsetTable 用于存放存储端最新消息的偏移

   3.4 Broker 端通过 consumerOffsetTable 和 offsetTable 找到需要开始消费的 offset

   3.5 根据 offset 从  ConsumerQueue 取出 CommitLog Offset

   3.6 根据 CommitLog Offset 从 CommitLog 中取出消息然后返回给 Consumer

   3.7 Consumer 接收到消息后回调 MessageListener 中设置好的方法

   3.8 不断重复 3.1-3.7

   



### 设计理念

------

1. 设计 Queue 的目的主要是为了提高消费的速度。当多个 Consumer 并发消费的时候，为了尽可能的避免两个消费者消费到了同一条数据，需要对整个 consumerOffsetTable 加锁，引入 Queue 之后则可以释放 consumerOffsetTable 锁。为什么说释放这和 RockcetMQ 的设计理念有关，尽可能保证速度的前提下去照顾到重复消费问题，也就是说 RocketMQ 无法做到防止重复消费。RocketMQ 引入 Queue 之后，在 Consumer 端完成 Queue 的负载均衡，这样不同的消费者访问不同的 Queue 就可以使得消费不冲突，避免 Broker 端加锁
2. consumerOffsetTable 中的 offset 并非是 CommitLog 中消息的 offset，如果是 CommitLog 中的 offset ，那么就还得有个额外的数据结构存放该 Queue 下一条数据的 offset。这里的 offset 指的是在 ConsumerQueue 文件中的第几条数据，ConsumerQueue 存放了消息在 CommitLog 中的偏移，且里面的数据被设计成了定长，所以可以通过 offset 直接定位到数据，然后再定位到 CommitLog 中的偏移去取数据
3. 设计 ConsumeQueue 的目的是为了提高查询的效率。因为所有的消息都被按顺序放到 CommitLog 文件中，如果要查找特定的消息就需要遍历一遍 CommitLog，要解决就需要引入索引，要避免服务宕机导致索引丢失则需要引入持久化
