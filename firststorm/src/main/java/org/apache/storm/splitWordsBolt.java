package org.apache.storm;

import java.util.*;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class splitWordsBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private Set<String> frequentWords = new HashSet<>();
    private String[] frqWord = new String[]{"today", "tonight"};

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        frequentWords.addAll(Arrays.asList(frqWord));
    }

    @Override
    public void execute(Tuple tuple) {
        String scentence = tuple.getString(0);
        String[] words = scentence.replaceAll("RT\\s", "").replaceAll("@\\S+\\s", "").replaceAll("https\\S+\\s*", "").replaceAll("\\wing", "").replaceAll("\\wed", "").split("\\W+");
//        System.out.println(Arrays.toString(words));
        for(String word: words){
            if(!frequentWords.contains(word) && word.length() > 5)
                _collector.emit(tuple, new Values(word));
        }
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        System.out.println("SplitWordBolt cleaning up......");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("splitword"));
    }

}
