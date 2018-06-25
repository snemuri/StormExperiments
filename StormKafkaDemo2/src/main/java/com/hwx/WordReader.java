package com.hwx;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by vkc on 6/12/17.
 */
public class WordReader implements IRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private String[] sentences =  new String[] {"In this tutorial, you will learn about the different features available in the HDF sandbox",
        "HDF stands for Hortonworks DataFlow",
        "HDF was built to make processing data-in-motion an easier task while also directing the data from source to the destination};\n"};


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        for (String line: sentences) {
            spoutOutputCollector.emit(new Values(line));
        }
    }

    public void ack(Object o) {
        System.out.println("OK:"+o);
    }

    public void fail(Object o) {
        System.out.println("FAIL:"+o);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}