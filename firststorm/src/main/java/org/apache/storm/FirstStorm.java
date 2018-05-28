package org.apache.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class FirstStorm {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterSpout());
        builder.setBolt("splitWords", new splitWordsBolt()).shuffleGrouping("twitter");
        builder.setBolt("wordCount", new wordcountBolt()).shuffleGrouping("splitWords");
        builder.setBolt("sortTopWord", new sortTopWord()).shuffleGrouping("wordCount");
        Config conf = new Config();
        conf.setDebug(true);
        if(args != null && args.length > 0) {
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("firststorm", conf, builder.createTopology());
            Utils.sleep(30000);
            cluster.killTopology("firststorm");
            cluster.shutdown();
        }
    }

}