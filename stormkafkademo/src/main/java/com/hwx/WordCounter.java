package com.hwx;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by vkc on 6/12/17.
 */
public class WordCounter implements IRichBolt {
    private OutputCollector outputCollector;
    private HashMap<String, Integer> counters;
    private int id;
    private String name;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector =outputCollector;
        this.counters = new HashMap<String, Integer>();
        this.id = topologyContext.getThisTaskId();
        this.name = topologyContext.getThisComponentId();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);

        if(!counters.containsKey(word)) {
            counters.put(word,1);
        } else {
            counters.put(word, counters.get(word) + 1);
        }
        System.out.println("Processing--> " + word);
        outputCollector.ack(tuple);

        printMap();

    }

    public void cleanup() {
        printMap();

    }

    private void printMap() {
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");
        for (Map.Entry<String, Integer> entry: counters.entrySet()) {
            System.out.println(entry.getKey()+ ":" + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
