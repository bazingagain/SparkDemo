package com.leon.learnspark.movies;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Created on 2018/4/21.
 *
 * @author Xiaolei-Peng
 */
public class MoviesApp {

    public static void main(String[] args) {
        String logFile = "FirstSpark/data/ml-100k/u.user";
        SparkConf conf = new SparkConf().setAppName("Simple");
        conf.set("spark.master", "local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> userData = sc.textFile(logFile).cache();
        System.out.println(userData.first());
        JavaRDD<String[]> user_field = userData.map(line -> line.split("|"));
        long num_users = user_field.map(fields -> fields[0]).count();
        long num_genders = user_field.map(fields -> fields[2]).distinct().count();
        long num_occupations = user_field.map(fields -> fields[3]).distinct().count();
        long num_zipcode = user_field.map(fields -> fields[4]).distinct().count();
        System.out.println("" +num_users + " " + num_genders + " " + num_occupations + " " + num_zipcode);
    }
}
