package org.apache.storm;

import java.util.Arrays;
import java.util.regex.*;
import java.io.*;

public class regexTest {
    public static void main(String[] args) throws IOException {
        FileReader in = new FileReader("TwitterStreamSample.txt");
        BufferedReader br = new BufferedReader(in);
        String tmp = br.readLine();
        while (tmp != null) {
            String[] words = tmp.replaceAll("RT\\s", "").replaceAll("@\\S+\\s", "").replaceAll("https\\S+\\s", "").split("\\W+");
            System.out.println(Arrays.toString(words));
            tmp = br.readLine();
        }
        in.close();
//        String test = "RT @lfnnng My sweet love, I want to marry you on April 13th in New York City!I look forward to that day coming soon!I\u2026 https://t.co/nM5bqg1yNj today I meet some one important";
//
//        String[] result = test.split("\\W+");
//        String tmp = test.replaceAll("RT @\\w+\\s", "").replaceAll("https\\S+", "");
//        System.out.println(tmp);
//        String[] result = tmp.split("\\W+", -1);
//        System.out.println(Arrays.toString(result));
    }
}
