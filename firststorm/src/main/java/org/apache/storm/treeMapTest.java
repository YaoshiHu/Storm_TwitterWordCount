package org.apache.storm;

import java.util.*;

public class treeMapTest {
    public static void main(String[] args){
        TreeMap<Integer, String> treemap = new TreeMap<>(((o1, o2) -> o2-o1));
        treemap.put(1234, "北京");
        treemap.put(345, "南京");
        treemap.put(664, "秦皇岛");
        treemap.put(1266, "济南");
        treemap.put(178, "天津");
        treemap.put(1789, "上海");
        treemap.put(1023, "苏州");
        Set<Map.Entry<Integer, String>> entrySet = treemap.entrySet();
        for(Map.Entry<Integer,String> ent : entrySet){
            System.out.println(ent.getValue() + " " + ent.getKey());
        }
    }
}
