package com.hwx;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by vkc on 6/12/17.
 */
public class WordNormalizer implements IRichBolt {
    private OutputCollector outputCollector;
    private static final org.slf4j.Logger LOGGER =
        LoggerFactory.getLogger("CUSTOMLOG");

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        LOGGER.info("WordNormalizer-prepare");
        this.outputCollector =outputCollector;
    }

    public void execute(Tuple tuple) {

        LOGGER.info("WordNormalizer-execute");

        String line = tuple.getString(0);
        String[] words = line.split(" ");

        for (String word: words) {
            if(!word.isEmpty()) {
                word = word.trim();

                outputCollector.emit(tuple, new Values(word));
            }

        }
        outputCollector.ack(tuple);


    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
