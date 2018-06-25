package com.hwx;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * Hello world!
 */
public class StormKafkaDemo {


    public static void main(String[] args) throws InterruptedException {


        try {
            StormSubmitter.submitTopology("StormKafkaDemo-Cluster-Topology", StormKafkaDemo.getConf(args),
                StormKafkaDemo.getTopology(args[0], args[1]));
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }


    }


    private static StormTopology getTopology(String bootstrapServers, String topics) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Kafka-reader", new KafkaSpout(getKafkaSpoutConfig(bootstrapServers, topics).build()), 1);
        builder.setBolt("Word-Normalizer", new WordNormalizer()).shuffleGrouping("Kafka-reader");
        builder.setBolt("Word-Counter", new WordCounter()).fieldsGrouping("Word-Normalizer", new Fields("word"));
        return builder.createTopology();
    }

    private static KafkaSpoutConfig.Builder getKafkaSpoutConfig(String bootstrapServers, String topics) {

          return new KafkaSpoutConfig.Builder<String, String>(bootstrapServers,topics).setGroupId("StormKafkaDemoGroup")
              .setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
              .setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    }

    private static Config getConf(String[] args) {
        Config conf = new Config();
        conf.setDebug(false);
        return conf;
    }
}

