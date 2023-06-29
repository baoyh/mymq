## 流程

------

1. Consumer 配置 Router 地址，要订阅的 Topic，所在的 Group

2. Consumer 配置消费接口 MessageListener 接收消息

3. Consumer 启动

   3.1 根据 Router 地址找到所有的 Broker，通过负载均衡算法选出一个 Broker 和 Queue

   3.2 将 Topic 、Group、Queue 发送给选出的 Broker 进行消费

   3.3 Broker 端存放了 <topic@group, <queueId, offset >> consumerOffsetTable 用于存放 Consumer 已经消费过的最新偏移

   3.4 根据 offset 从  ConsumeQueue 取出 CommitLog Offset

   3.5 根据 CommitLog Offset 从 CommitLog 中取出消息然后返回给 Consumer

   3.6 重复 3.4-3.5 直到读取到 ConsumeQueue 文件的末尾, 将所有需要消费的数据都取出

   3.7 Consumer 接收到消息后回调 MessageListener 中设置好的方法

   3.8 不断重复 3.1-3.7

   



## 设计理念

------

1. 设计 Queue 的目的主要是为了提高消费的速度。当多个 Consumer 并发消费的时候，为了尽可能的避免两个消费者消费到了同一条数据，需要对整个 consumerOffsetTable 加锁，引入 Queue 之后则可以释放 consumerOffsetTable 锁。为什么说释放这和 RockcetMQ 的设计理念有关，尽可能保证速度的前提下去照顾到重复消费问题，也就是说 RocketMQ 无法做到防止重复消费。RocketMQ 引入 Queue 之后，在 Consumer 端完成 Queue 的负载均衡，这样不同的消费者访问不同的 Queue 就可以使得消费不冲突，避免 Broker 端加锁
2. consumerOffsetTable 中的 offset 并非是 CommitLog 中消息的 offset，如果是 CommitLog 中的 offset ，那么就还得有个额外的数据结构存放该 Queue 下一条数据的 offset。这里的 offset 指的是在 ConsumerQueue 文件中的第几条数据，ConsumerQueue 存放了消息在 CommitLog 中的偏移，且里面的数据被设计成了定长，所以可以通过 offset 直接定位到数据，然后再定位到 CommitLog 中的偏移去取数据
3. 设计 ConsumeQueue 的目的是为了提高查询的效率。因为所有的消息都被按顺序放到 CommitLog 文件中，如果要查找特定的消息就需要遍历一遍 CommitLog，要解决就需要引入索引，要避免服务宕机导致索引丢失则需要引入持久化


## 疑问点

------

### Consumer 消费消息基于 Pull, 如何保证实时性? 

Push 是服务端主动推送, 可以保证实时性, 消费端只需要接收消息即可; 缺点是服务端不知道消费端的消费能力, 可能导致消息在消费端堆积
Pull 是消费端主动拉取, 消费端可以在消费完后再发起拉取, 不会导致消息堆积; 缺点是如果不停的拉取会导致在没有新数据时浪费资源, 间隔拉取则不好保证实时性

为了既保证消息的实时性, 又不会造成消息端消息堆积, 可以采用 Push 和 Pull 结合的方式, 也被称作长轮询, 方法如下:
1. 由消费端主动 Pull
2. 如果有数据, 直接返回给消费端; 如果没有找到数据, 服务端会 hold 请求, 当有新数据进来后再将数据给消费端
3. 如果长时间没有消费到数据, 服务端也会返回信息给消费端, 消费端则会将请求加到请求队列里等待再次发起请求

### RocketMQ 中如何实现长轮询

#### PullMessageService

执行拉取的方法是 PullMessageService.run()
PullMessageService 维护了一个阻塞队列 pullRequestQueue 用于存放请求, 拉取时从中拿到请求去执行拉取操作
````
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }
````

#### PullMessageProcessor

Broker 中的 PullMessageProcessor.processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) 用于处理消费请求
下面重点看下当消息找不到时的代码
````
    case ResponseCode.PULL_NOT_FOUND:
   
        if (brokerAllowSuspend && hasSuspendFlag) {
            long pollingTimeMills = suspendTimeoutMillisLong;
            if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
            }
   
            String topic = requestHeader.getTopic();
            long offset = requestHeader.getQueueOffset();
            int queueId = requestHeader.getQueueId();
            PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills,
                this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
            this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
            response = null;
            break;
        }
````
brokerAllowSuspend 和 hasSuspendFlag 用于标识是否需要 hold 住请求, brokerAllowSuspend 在 Broker 中控制, hasSuspendFlag 由消费端传过来
suspendTimeoutMillisLong 表示 hold 的超时时间, 由消费端传过来, 位于 PullMessageRequestHeader 中, 在 4.9.4 版本中默认为 20s
最后执行 suspendPullRequest 来暂停请求, 同时将 response 置空

#### PullRequestHoldService

````
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }
````
suspendPullRequest 的作用是请求放入缓存中, ManyPullRequest 中维护了一个请求列表 ArrayList<PullRequest> pullRequestList, 由于是非线程安全的, 所有方法都加上了 synchronized

````
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }
````
有一个线程每隔 5s 或 1s 去执行一次 checkHoldRequest(), 虽然是 waitForRunning, 但本人并没有找到执行 PullRequestHoldService.wakeup() 的地方, 也就是说每次都会超时

````
    protected void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }
````
checkHoldRequest 的作用是根据解析 topic 和 queueId, 并获取最新的偏移 offset, 执行 notifyMessageArriving

````
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }

                    if (newestOffset > request.getPullFromThisOffset()) {
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }

                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
````
整个方法可以分为三块
1. 遍历缓存中的 PullRequest 列表, 根据请求中的 pullFromThisOffset 和 ConsumeQueue 中的最新偏移 newestOffset 比较是否有新的数据
2. 如果有新数据且可以匹配上 tag 和 properties, 则执行 executeRequestWhenWakeup 去处理 hold 住的请求
3. 如果 hold 时间超过了 suspendTimestamp, 也会去执行 executeRequestWhenWakeup

````
    public void executeRequestWhenWakeup(final Channel channel,
        final RemotingCommand request) throws RemotingCommandException {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false);

                    if (response != null) {
                        response.setOpaque(request.getOpaque());
                        response.markResponseType();
                        try {
                            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        log.error("processRequestWrapper response to {} failed",
                                            future.channel().remoteAddress(), future.cause());
                                        log.error(request.toString());
                                        log.error(response.toString());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("processRequestWrapper process request over, but response failed", e);
                            log.error(request.toString());
                            log.error(response.toString());
                        }
                    }
                } catch (RemotingCommandException e1) {
                    log.error("excuteRequestWhenWakeup run", e1);
                }
            }
        };
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

````
executeRequestWhenWakeup 方法会再次调用 processRequest, brokerAllowSuspend 参数被设置成了 false, 不会再去 hold 

##### DefaultMQPushConsumerImpl

````
   case NO_MATCHED_MSG:
       pullRequest.setNextOffset(pullResult.getNextBeginOffset());

       DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

       DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
       break;
````
超时后依然没有获取到消息时, 服务端 Broker 会将请求结果发送给消费端, 消费端接收后会执行上方代码将 pullRequest 重新提交到请求队列中

#### ReputMessageService

从上面的分析中可以得知 PullRequestHoldService 是间歇性的去执行 executeRequestWhenWakeup, 并不能保证实时性, ReputMessageService 可以弥补这一点

````
   public void run() {
      DefaultMessageStore.log.info(this.getServiceName() + " service started");
   
      while (!this.isStopped()) {
          try {
              Thread.sleep(1);
              this.doReput();
          } catch (Exception e) {
              DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
          }
      }
   
      DefaultMessageStore.log.info(this.getServiceName() + " service end");
   }
````

ReputMessageService 中有一个线程每隔 1ms 就会执行一次 doReput 

```` 
   private void doReput() {
      ....
      for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
   
          if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
              && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
              break;
          }
   
          SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
          if (result != null) {
              try {
                  this.reputFromOffset = result.getStartOffset();
   
                  for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                      DispatchRequest dispatchRequest =
                          DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                      int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();
   
                      if (dispatchRequest.isSuccess()) {
                          if (size > 0) {
                              DefaultMessageStore.this.doDispatch(dispatchRequest);
   
                              if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                      && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                                      && DefaultMessageStore.this.messageArrivingListener != null) {
                                  DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                      dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                      dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                      dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                  notifyMessageArrive4MultiQueue(dispatchRequest);
                              }
````

上面只截取了 doReput 方法的一部分, 逻辑上是先检查是否有新的数据到达, 如果有就执行 messageArrivingListener.arriving

#### NotifyMessageArrivingListener

````
   public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
     long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
     this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode,
         msgStoreTime, filterBitMap, properties);
   }
````

调用 pullRequestHoldService.notifyMessageArriving 去向消费端立刻发送消息


### 总结

1. 消费端发起请求时会带上 hasSuspendFlag 表示是否需要服务端 hold 未消费到消息的请求, 默认为 true; suspendTimeoutMillisLong 表示 hold 的超时时间, 默认为 20s
2. 服务端 hold 请求后, 每隔 5s 或 1s 会做一次检查, 如果有新消息进来就会消息发送给消费端, 同时如果 hold 超期也会将请求返回给消费端
3. 为了保证实时性, 会有一个线程每隔 1ms 检查是否有新数据进来, 有的话也会立即向消费端发消息
4. 消费端在接收到 response 后如果依然没有消息, 则会再次将请求提交到请求队列中
5. 从功能设计的角度看, ReputMessageService.run 的 1ms 轮询是为了保证实时性, 如果一直没有消息, 也就不会执行 notifyMessageArriving, 当然也就无法做超时检查;
   PullRequestHoldService.run 的作用更像是为了做超时检查, 新消息大概率在其等待的时间点产生, 也就会由 ReputMessageService 去执行 notifyMessageArriving
   但无论是新数据进来了还是超时了最后都需要去查找一遍数据, 然后返回给消费端 
6. ReputMessageService.run 的 1ms 轮询当没有新数据时会造成资源浪费。如果在 store message 完成时去触发 notifyMessageArriving 是否会更高效
