









# 事务

Kafka 从 0.11 版本开始引入了事务支持。事务可以保证 Kafka 在 Exactly Once 语义的基 础上，生产和消费可以跨分区和会话，要么全部成功，要么全部失败。

## Producer 事务

在跨分区跨会话的场景下，保证生产的消息精准一次性写入kafka



## Consumer 事务

保证消费的消息只消费一次。







# 总结

Kafka 中的 ISR(InSyncRepli)、OSR(OutSyncRepli)、AR(AllRepli)代表什么？

ISR 指的是和 leader 保持同步的 follower 集合。

OSR 指的是不和 leader 保持同步的 follower 集合。

AR 指的是所有的副本集合。

ISR + OSR = AR



Kafka 中的 HW、LEO 等分别代表什么？

HW（High Watermark，高损备）：指的是消费者能见到的最大的 offset，ISR 队列中最小的 LEO。

LEO：指的是每个副本最大的 offset；



Kafka 中是怎么体现消息顺序性的？

分区内有序。



Kafka 中的分区器、序列化器、拦截器的作用？它们之间的处理顺序是什么？

执行顺序：拦截器、序列化器、分区器



Kafka 生产者客户端的整体结构是什么样子的？使用了几个线程来处理？分别是什么？

使用2个线程来处理，分别是main线程和send线程



消费者提交消费位移时提交的是当前消费到的最新消息的 offset 还是 offset+1？

offset+1



有哪些情形会造成重复消费？

先处理数据再提交offset



那些情景会造成消息漏消费？

先提交offset后处理数据



当使用 kafka-topics.sh 创建（删除）了一个 topic 之后，Kafka 背后会执行什么逻辑？

1）会在 zookeeper 中的/brokers/topics 节点下创建一个新的 topic 节点，如： /brokers/topics/first。

2）触发 Controller 的监听程序。

3）kafka Controller 负责 topic 的创建工作，并更新 metadata cache。



topic 的分区数可不可以增加？如果可以怎么增加？如果不可以，那又是为什么？

可以。



topic 的分区数可不可以减少？如果可以怎么减少？如果不可以，那又是为什么？

不可以。已经存在的分区无法处理。



Kafka 有内部的 topic 吗？如果有是什么？有什么所用？

有， __consumer_offsets，用于维护offset。



Kafka 分区分配的概念

Range，RoundRobin，Sticky



Kafka 中有那些地方需要选举？这些地方的选举策略又有哪些？

kafka controller 和 leader

controller 靠争抢资源的方式选举。

leader 通过ISR进行选举。



失效副本是指什么？有那些应对措施？



Kafka 的哪些设计让它有如此高的性能？

分布式架构、顺序写磁盘、零拷贝。
