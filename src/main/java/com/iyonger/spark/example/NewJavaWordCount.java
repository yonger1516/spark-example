package com.iyonger.spark.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Char;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by yongfu on 10/19/2016.
 */
public class NewJavaWordCount {
    private static final Logger logger = LoggerFactory.getLogger(NewJavaWordCount.class);
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("not enough parameters");
            System.exit(1);
        }
        SparkSession spark = SparkSession.builder().appName("New Java word count").getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String line = replaceNonLetter(s);
                logger.info("line after replacement:"+line);
                return Arrays.asList(SPACE.split(line)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();

    }

    public static String replaceNonLetter(String line) {
        for (char c : line.toCharArray()) {
            if (c < 65 || (c > 90 && c < 97) || c > 122) {
                line.replace(c, ' ');
            }
        }

        return line;
    }


}
