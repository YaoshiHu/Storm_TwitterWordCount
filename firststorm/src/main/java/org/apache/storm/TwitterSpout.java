package org.apache.storm;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.utils.Utils;
import twitter4j.*;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterSpout extends BaseRichSpout{
    private SpoutOutputCollector _collector;
    private LinkedBlockingQueue<String> queue = null;
    private TwitterStream _twitterStream;

    // the method to create a stream
    public void nextTuple() {
        String ret = queue.poll();

        if (ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));
        }
    }
    //  open method is called when a task for this component is initialized within a worker on the cluster.
    //  conf - The Storm configuration for this spout. This is the configuration provided to the topology merged in
    //  with cluster configuration on this machine.
    //  context - This object can be used to get information about this task’s place within the topology, including
    //  the task id and component id of this task, input and output information, etc.
    //  collector - The collector is used to emit tuples from this spout. Tuples can be emitted at any time, including
    //  the open and close methods. The collector is thread-safe and should be saved as an instance variable of this
    //  spout object.

    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
        queue = new LinkedBlockingQueue<String>(1000);
        _collector = arg2;
        String[] keyWords = {"Washington", "New York", "Illinois", "Chicago", "Seattle", "San Fransisco", "Boston"};
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
//                System.out.println(status.getText());
                queue.offer(status.getText());
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        _twitterStream = new TwitterStreamFactory().getInstance();
        _twitterStream.addListener(listener);

        FilterQuery query = new FilterQuery().track(keyWords);
        _twitterStream.filter(query);
    }
    //  void close()
    //  Called when an ISpout is going to be shutdown. There is no guarentee that close will be called, because the
    //  supervisor kill -9’s worker processes on the cluster.
    //  The one context where close is guaranteed to be called is a topology is killed when running Storm in local mode.
    public void close(){
        _twitterStream.shutdown();
    }
    // OutputFieldsDeclarer: used to declare streams and their schemas
    // here we use the .declare() method to declare a stream
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("twitterString"));
    }
}
