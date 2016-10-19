package com.iyonger.spark.example;

import org.apache.spark.sql.SparkSession;

/**
 * Created by yongfu on 10/18/2016.
 */
public class TeraGen {
    public static void main(String[] args){
        if (args.length<1){
            System.out.println("not enough parameters");
            System.exit(1);
        }
        SparkSession spark=SparkSession.builder().appName("TeraGen").getOrCreate();


    }
}
