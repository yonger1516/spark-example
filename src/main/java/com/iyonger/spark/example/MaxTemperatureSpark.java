package com.iyonger.spark.example;

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by yongfu on 7/4/2016.
 */
public class MaxTemperatureSpark {
    String input;
    String output;

    public MaxTemperatureSpark(String input,String output){
        this.input=input;
        this.output=output;
    }

    public void run(){
        SparkConf conf=new SparkConf();
        JavaSparkContext sc=new JavaSparkContext("local","MaxTemperatureSpark",conf);

        JavaRDD<String> lines=sc.textFile(input);
        JavaRDD<String[]> records=lines.map(new Function<String, String[]>() {
            public String[] call(String s) throws Exception {
                return s.split("\t");
            }
        });

        JavaRDD<String[]> filtered=records.filter(new Function<String[], Boolean>() {
            public Boolean call(String[] records) throws Exception {
                return records[1]!="9999" && records[2].matches("[01459]");
            }
        });

        JavaPairRDD<Integer,Integer> tuples=filtered.mapToPair(
                new PairFunction<String[], Integer, Integer>() {
                    public Tuple2<Integer, Integer> call(String[] records) throws Exception {
                        return new Tuple2<Integer, Integer>(
                                Integer.parseInt(records[0]),Integer.parseInt(records[1]));
                    }
                }
        );

        JavaPairRDD<Integer,Integer> maxTemps=tuples.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return Math.max(i1,i2);
                    }
                }
        );

        maxTemps.saveAsTextFile(output);


    }
}
