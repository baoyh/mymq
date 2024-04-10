1. Leader 挂了之后，集群重新选举新的 Leader 后，当老 Leader 恢复时，会发生什么

   这里称老的 Leader 为 OL，新的为 NL

   OL 恢复后，重新发送 heartbeats，会收到已经存在新 Leader 的回复。这种情况只要将 OL 设置为 Follower 即可

   