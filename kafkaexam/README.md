# Kafka示例程序

Kafka的版本为`0.10.0.0`.

## 示例1

本示例主要来自官网的[examples](https://github.com/apache/kafka/tree/0.10.0/examples/src/main/java/kafka/examples)示例，并了少许改进。

- [producer](https://github.com/wangzzu/bigdata-examples/tree/master/kafkaexam/src/main/java/producer/simple)
- [consumer](https://github.com/wangzzu/bigdata-examples/tree/master/kafkaexam/src/main/java/consumer/simple)

运行方法：

```sh
# producer
java -cp kafkaexam.jar producer.simple.KafkaProducerDemo test1 async localhost:9092
# consumer
java -cp kafkaexam.jar consumer.simple.Consumer test1 localhost:9092
```


