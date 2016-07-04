package com.iyonger.spark.example;

/**
 * Created by yongfu on 7/4/2016.
 */
public class SparkMain {
    public static void main(String[] args){
        if (args.length!=2){
            System.err.println("usage: xxx <input path> <output path>");
            System.exit(-1);
        }

        MaxTemperatureSpark maxTemperatureSpark=new MaxTemperatureSpark(args[0],args[1]);
        maxTemperatureSpark.run();
    }

}
