package com.iyonger.spark.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Char;

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

    }

    public static String replaceNonLetter(String line) {
        for (char c : line.toCharArray()) {
            if (c < 65 || (c > 90 && c < 97) || c > 122) {
                line.replace(c, ' ');
            }
        }

        return line;
    }

    public static List<Character> getSplitChars(String word) {
        List<Character> chars = new ArrayList<Character>();
        for (char c : word.toCharArray()) {
            if (c < 65 || (c > 90 && c < 97) || c > 122) {
                chars.add(c);
            }
        }
        return chars;
    }

}
