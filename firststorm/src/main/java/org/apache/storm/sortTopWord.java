package org.apache.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class sortTopWord extends BaseRichBolt{
    private OutputCollector _collector;
    private String[] topWords = new String[3];
    private long[] counts = new long[3];
//    private Set<String> set= new HashSet<>();
//    private TreeMap<Long, String> orderMap;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
//        orderMap = new TreeMap<>(((o1, o2) -> o2.compareTo(o1)));
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        long count = tuple.getLong(1);
//        System.out.println(word+Long.toString(count));
        if(!word.equals(topWords[0]) && !word.equals(topWords[1]) && !word.equals(topWords[2]))
            if(count>counts[0]){
                counts[2] = counts[1];
                counts[1] = counts[0];
                counts[0] = count;
                topWords[2] = topWords[1];
                topWords[1] = topWords[0];
                topWords[0] = word;
            }
            else if(count>counts[1]){
                counts[2] = counts[1];
                counts[1] = count;
                topWords[2] = topWords[1];
                topWords[1] = word;
            }
            else if(count>counts[2]){
                counts[2] = count;
                topWords[2] = word;
            }
        System.out.println("The top 1 word is \"" + topWords[0] + "\" with count " + Long.toString(counts[0]));
        System.out.println("The top 2 word is \"" + topWords[1] + "\" with count " + Long.toString(counts[1]));
        System.out.println("The top 3 word is \"" + topWords[2] + "\" with count " + Long.toString(counts[2]));
        System.out.println("===========================================================================");
//        orderMap.put(count, word);
//        if(!word.equals("") && word.length()>2)_collector.emit(tuple, new Values(word));
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        System.out.println("sortTopWordBolt cleaning up......");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topWords"));
    }
}


