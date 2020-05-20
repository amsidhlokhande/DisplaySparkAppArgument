package com.amsidh.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DisplaySparkAppArgument {
    public static void main(String[] args) {
        String inputFile = args[0];
        String inputJson = args[1];
        System.out.println("----Input Json Started---------");
        System.out.println(inputJson);
        System.out.println("----Input Json Ended---------");
        SparkConf sparkConf = getSparkConf();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = javaSparkContext.textFile(inputFile);
        System.out.println("Display the content of the file:");
        lines.collect().forEach(System.out::println);
    }


    private static SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf();
        //sparkConf.setMaster("local");
        sparkConf.setAppName("DisplaySparkAppArgument");
        return sparkConf;
    }

}
