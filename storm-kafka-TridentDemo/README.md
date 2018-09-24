

``` bash
./kafka-topics.sh --create --zookeeper c420-node2.squadron-labs.com:2181 --replication-factor 2 --partitions 3 --topic sourceTopic1

./kafka-topics.sh --create --zookeeper c420-node2.squadron-labs.com:2181 --replication-factor 2 --partitions 3 --topic destTopic1


./kafka-topics.sh --create --zookeeper c420-node2.squadron-labs.com:2181 --replication-factor 2 --partitions 3 --topic sourceTridentTopic1

./kafka-topics.sh --create --zookeeper c420-node2.squadron-labs.com:2181 --replication-factor 2 --partitions 3 --topic destTridentTopic1
```

Storm core Demo:
 ```
 /usr/hdp/current/storm-client/bin/storm jar kafka-storm-kafka-0.0.1-SNAPSHOT.jar com.vkc.KafkaStormKafkaTopology c420-node2.squadron-labs.com:6667 sourceTopic1 destTopic1 --jars ./storm-kafka-client-1.1.0.2.6.4.0-91.jar,/usr/hdp/current/kafka-broker/libs/kafka-clients-0.10.1.2.6.4.0-91.jar
```


Storm Trident Kafka Proucer Demo:
```
/usr/hdp/current/storm-client/bin/storm jar kafka-storm-kafka-0.0.1-SNAPSHOT.jar com.vkc.FixedSpoutTridentKafkaBoltTopology c420-node2.squadron-labs.com:6667 sourceTridentTopic1 destTridentTopic1 --jars ./storm-kafka-client-1.1.0.2.6.4.0-91.jar,/usr/hdp/current/kafka-broker/libs/kafka-clients-0.10.1.2.6.4.0-91.jar
```

Kafkaspout storm Trident Kafka Proucer Demo :
 ```
 /usr/hdp/current/storm-client/bin/storm jar kafka-storm-kafka-0.0.1-SNAPSHOT.jar com.vkc.KafkaSpoutTridentKafkaBoltTopology c420-node2.squadron-labs.com:6667 sourceTridentTopic1 destTridentTopic1 --jars ./storm-kafka-client-1.1.0.2.6.4.0-91.jar,/usr/hdp/current/kafka-broker/libs/kafka-clients-0.10.1.2.6.4.0-91.jar
```