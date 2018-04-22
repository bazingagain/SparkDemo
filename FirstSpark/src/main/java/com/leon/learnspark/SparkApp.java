package com.leon.learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created on 2018/4/20.
 *
 * @author Xiaolei-Peng
 */
public class SparkApp {

    public static void main(String[] args) {
        String logFile = "FirstSpark/data/test.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter((s) -> s.contains("a")).count();
        long numBs = logData.filter((s) -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.stop();
    }

}
