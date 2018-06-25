Using 
 <dependency>
          <groupId>org.apache.storm</groupId>
          <artifactId>storm-kafka-client</artifactId>
          <version>1.1.0</version>
      </dependency>


./storm jar /root/stormdemo/StormKafkaDemo-1.0-SNAPSHOT-jar-with-dependencies.jar  com.hwx.StormKafkaDemo hdp2603node1.openstacklocal:6667 test1 --jars /root/stormdemo/storm-kafka-client-1.1.0-SNAPSHOT.jar,/usr/hdp/current/kafka-broker/libs/kafka-clients-0.10.1.2.6.0.3-8.jar
