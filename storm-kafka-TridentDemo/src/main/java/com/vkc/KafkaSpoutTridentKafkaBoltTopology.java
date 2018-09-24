package com.vkc;



import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class KafkaSpoutTridentKafkaBoltTopology {

    private static final Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new JustValueFunc();

    protected static KafkaSpoutConfig<String, String> createKafkaSpoutConfig(String bootstrapServers, String topic) {
        return KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup_" + System.nanoTime())
                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200)
                .setRecordTranslator(JUST_VALUE_FUNC, new Fields("sentence"))
                .setRetry(KafkaSpoutTridentKafkaBoltTopology.createRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }


    private static KafkaTridentSpoutOpaque<String, String> createKafkaTridentSpoutOpaque(KafkaSpoutConfig<String, String> spoutConfig) {
        return new KafkaTridentSpoutOpaque<>(spoutConfig);
    }

    /**
     * Needs to be serializable.
     */
    private static class JustValueFunc implements Func<ConsumerRecord<String, String>, List<Object>>, Serializable {

        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.value());
        }
    }

    protected static KafkaSpoutRetryService createRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(new KafkaSpoutRetryExponentialBackoff.TimeInterval(500L, TimeUnit.MICROSECONDS),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

        if (args.length < 3) {
            System.out.println("Usage :<bootstrap.servers> <sourcetopic> <destination topic> ");
            System.exit(0);
        }

        String bootstrapservers = args[0];
        String sourceTopic = args[1];
        String destTopic = args[2];

        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfig(bootstrapservers, sourceTopic);

        ITridentDataSource kafkaTridentSpoutOpaquespout =  createKafkaTridentSpoutOpaque(spoutConfig);

        TridentTopology topology = new TridentTopology();
        Stream wordsStream = topology.newStream("kafkaTridentSpoutOpaquespout", kafkaTridentSpoutOpaquespout)
                .each(new Fields("sentence"), new SplitSentence(), new Fields("word"))
                .parallelismHint(16);



        Properties props = getKafkaProducerProperties(bootstrapservers);
        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(props)
                .withKafkaTopicSelector( new DefaultTopicSelector(destTopic))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "word"));

        wordsStream.partitionPersist(stateFactory,  new Fields("word"), new TridentKafkaStateUpdater(), new Fields());

        Config conf = new Config();
        StormSubmitter.submitTopology("KafkaSpoutTridentKafkaBoltTest", conf, topology.build());
    }

    private static Properties getKafkaProducerProperties(String bootstrapservers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapservers);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


    private static class SplitSentence extends BaseFunction {

        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String sentence = tridentTuple.getString(0);
            for (String word : sentence.split(" ")) {
                tridentCollector.emit(new Values(word));
            }
        }
    }
}
