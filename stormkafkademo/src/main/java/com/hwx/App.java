package com.hwx;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Hello world!
 */
public class App {


    private static final Logger logger = Logger.getLogger("CustomLog");

    public static void main(String[] args) throws InterruptedException {


        initFileLogger(args[0]);


        logger.info("In App.main  ");
        if (Boolean.valueOf(args[1])) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Getting-Started-LocalTopology", App.getConf(args), App.getTopology());
            Thread.sleep(10000);
            localCluster.shutdown();
        } else {

            try {
                StormSubmitter.submitTopology("Getting-Started-Cluster-Topology", App.getConf(args), App.getTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }


    }

    private static void initFileLogger(String arg) {
        try {
            FileHandler fh = new FileHandler(arg);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static StormTopology getTopology() {
        logger.info("In App.getTopology  ");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Word-reader", new WordReader());
        builder.setBolt("Word-Normalizer", new WordNormalizer()).shuffleGrouping("Word-reader");
        builder.setBolt("Word-Counter", new WordCounter()).fieldsGrouping("Word-Normalizer", new Fields("word"));
        return builder.createTopology();
    }

    private static Config getConf(String[] args) {
        logger.info("In App.getConf  ");
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(false);
        return conf;
    }
}

