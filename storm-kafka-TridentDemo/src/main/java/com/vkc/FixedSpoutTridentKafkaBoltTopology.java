package com.vkc;



import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

public class FixedSpoutTridentKafkaBoltTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

        if (args.length < 2) {
            System.out.println("Usage :<bootstrap.servers>  <destination topic> ");
            System.exit(0);
        }

        String bootstrapservers = args[0];
        String destTridentTopic = args[1];

        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", "1"),
                new Values("trident", "1"),
                new Values("needs", "1"),
                new Values("javadoc", "1")
        );
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);
        //set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapservers);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(props)
                .withKafkaTopicSelector( new DefaultTopicSelector(destTridentTopic))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
        stream.partitionPersist(stateFactory, fields, new TridentKafkaStateUpdater(), new Fields());

        Config conf = new Config();
        StormSubmitter.submitTopology("FixedSpoutTridentKafkaBoltTest", conf, topology.build());
    }
}
