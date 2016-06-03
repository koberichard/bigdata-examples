# Kafka示例程序

Kafka的版本为`0.10.0.0`.

## 示例1

本示例主要来自官网的[examples](https://github.com/apache/kafka/tree/0.10.0/examples/src/main/java/kafka/examples)示例，并进行了少许改进。

- [producer](https://github.com/wangzzu/bigdata-examples/tree/master/kafkaexam/src/main/java/producer/simple)
- [consumer](https://github.com/wangzzu/bigdata-examples/tree/master/kafkaexam/src/main/java/consumer/simple)

运行方法：

```sh
# producer
java -cp kafkaexam.jar producer.simple.KafkaProducerDemo test1 async localhost:9092
# consumer
java -cp kafkaexam.jar consumer.simple.Consumer test1 localhost:9092
```

producer端的日志输出：

```
message(1, Message_1) sent to partition(0), offset(0) in 145 ms
message(2, Message_2) sent to partition(0), offset(1) in 10 ms
message(3, Message_3) sent to partition(0), offset(2) in 10 ms
message(4, Message_4) sent to partition(0), offset(3) in 10 ms
message(5, Message_5) sent to partition(0), offset(4) in 9 ms
...
```

consumer端的日志输出：

```
Received message: (1, Message_1) at offset 0
Received message: (2, Message_2) at offset 1
Received message: (3, Message_3) at offset 2
Received message: (4, Message_4) at offset 3
Received message: (5, Message_5) at offset 4
...
```

## 示例2

本例是根据一些常用的使用场景编写。

### producer

实现功能([程序]())：

- 读取一个目录，可以设置是否监控该目录，如果选择一直监控该目录（将`isMonitor`设置为true），producer就会一直读取该目录；
- 可以设置启动多个线程，如果设置多线程（也就是`threadNum`设置大于1），此时，需要指出每个线程的foldPath和partition，中间使用`,`作为分割符，后面会有例子介绍；