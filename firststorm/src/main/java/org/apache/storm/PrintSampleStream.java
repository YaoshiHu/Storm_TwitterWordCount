package org.apache.storm;

import twitter4j.*;
import java.io.*;

public final class PrintSampleStream {
    /**
     * Main entry of this application.
     *
     * @param args arguments doesn't take effect with this example
     */
    public static void main(String[] args) throws IOException{
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        BufferedWriter writer = new BufferedWriter(new FileWriter("TwitterStreamSample.txt"));
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
//                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                String str = status.getText();
                System.out.println(status);
                try {
                    writer.append(str);
                    writer.append("\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
        twitterStream.addListener(listener);

        FilterQuery filter = new FilterQuery();
        String[] keywords = {"Washington", "New York", "Illinois", "Chicago", "Seattle", "San Fransisco", "Boston"};
        filter.track(keywords);
        filter.language("en");
        twitterStream.filter(filter);

    }
}