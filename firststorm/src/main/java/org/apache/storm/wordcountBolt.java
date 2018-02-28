package org.apache.storm;

import java.util.*;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class wordcountBolt extends BaseRichBolt {
    private HashMap<String, Long> countMap;
    private OutputCollector _collector;
    @Override
    // Called when a task for this component is initialized within a worker on the cluster.
    // It provides the bolt with the environment in which the bolt executes.
    // stormConf - The Storm configuration for this bolt. This is the configuration provided to the topology merged
    // in with cluster configuration on this machine.
    // context - This object can be used to get information about this task’s place within the topology, including
    // the task id and component id of this task, input and output information, etc.
    // collector - The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including
    // the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        // This output collector exposes the API for emitting tuples from an IRichBolt. This is the core API for
        // emitting tuples. For a simpler API, and a more restricted form of stream processing, see IBasicBolt and BasicOutputCollector.
        _collector = collector;
        countMap = new HashMap<>();
    }

    @Override
    // Process a single tuple of input. The Tuple object contains metadata on it about which component/stream/task
    // it came from. The values of the Tuple can be accessed using Tuple#getValue. The IBolt does not have to process
    // the Tuple immediately. It is perfectly fine to hang onto a tuple and process it later (for instance, to do an aggregation or join).
    //
    // Tuples should be emitted using the OutputCollector provided through the prepare method. It is required that all
    // input tuples are acked or failed at some point using the OutputCollector. Otherwise, Storm will be unable to
    // determine when tuples coming off the spouts have been completed.
    //
    // For the common case of acking an input tuple at the end of the execute method, see IBasicBolt which automates this.
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Long c = countMap.getOrDefault(word, (long)0);
        ++c;
//        System.out.println(word);
        countMap.put(word, c);
        _collector.emit(tuple, new Values(word, c));
        _collector.ack(tuple);
    }

    @Override
    // not guaranteed to be called, usually used in local mode
    public void cleanup() {
        System.out.println("WordCountBolt cleaning up......");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "Count"));
    }

}