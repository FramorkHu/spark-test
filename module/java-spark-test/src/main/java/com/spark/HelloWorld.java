package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by huyan on 16/4/21.
 */
public class HelloWorld {


    public static void main(String args[]){

        String sparkHome = "/usr/local/spark-1.3.1-bin-hadoop2.4";
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaHelloWorld");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> fileRdd = sparkContext.textFile(sparkHome + "/README.md");
        JavaRDD<Map<String, Integer>> mapJavaRDD = fileRdd.map(new Function<String, Map<String, Integer>>() {


            @Override
            public Map<String, Integer> call(String line) throws Exception {
                String[] worlds = line.split(" ");
                Map<String, Integer> map = new LinkedHashMap<String, Integer>();
                for (String world : worlds) {
                    if (map.get(world) == null) {
                        map.put(world, 1);
                    } else {
                        map.put(world, map.get(world) + 1);
                    }
                }

                return map;
            }
        });

        Map<String, Integer> result = mapJavaRDD.reduce(new Function2<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>>() {

            @Override
            public Map<String, Integer> call(Map<String, Integer> stringIntegerMap, Map<String, Integer> stringIntegerMap2) throws Exception {
                Map<String, Integer> result = new LinkedHashMap<String, Integer>();

                for (Map.Entry<String, Integer> entry : stringIntegerMap.entrySet()) {
                    String world = entry.getKey();
                    int count = entry.getValue();
                    if (stringIntegerMap2.containsKey(world)) {
                        result.put(world, stringIntegerMap2.get(world) + count);
                        stringIntegerMap2.remove(world);
                    } else {
                        result.put(world, count);
                    }
                }
                result.putAll(stringIntegerMap2);
                return result;
            }
        });
        for (Map.Entry<String, Integer> entry : result.entrySet()){
            System.out.println(entry.getKey()+" "+entry.getValue());
        }



    }
}
