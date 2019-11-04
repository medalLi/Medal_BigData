package com.case2;

import breeze.linalg.sum;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author medal
 * @create 2019-04-12 14:37
 **/
public class StaticsHeight {
    public static void main(String[] args) {
       // MokeData.createData(10);
        SparkConf conf = new SparkConf().setAppName("StaticsHeight").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile("SparkJava" +
                "\\src\\main\\java\\com\\case2\\data.txt");

        JavaRDD<String> manData = lines.filter(new Function<String,Boolean>(){

            public Boolean call(String s) throws Exception {

                return s.contains("M");
            }
        });

        JavaRDD<String> womanData = lines.filter(new Function<String,Boolean>(){

            public Boolean call(String s) throws Exception {

                return s.contains("F");
            }
        });

       // final Accumulator<Integer> sum_man = sc.accumulator(0);
        //final Accumulator<Integer> sum_woman = sc.accumulator(0);

        long countMan = manData.count();
        long countWoman = womanData.count();

        JavaRDD<Integer> manheigh = manData.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.parseInt(s.split("\t")[2]);
            }
        });
        JavaRDD<Integer> womanheigh = womanData.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.parseInt(s.split("\t")[2]);
            }
        });

        int maxmanheigh = manheigh.takeOrdered((int)countMan).get((int)countMan-1);
        int minmanheigh = manheigh.takeOrdered((int)countMan).get(0);
        int maxwomanheigh = womanheigh.takeOrdered((int)countWoman).get((int)countWoman-1);
        int minwomanheigh = womanheigh.takeOrdered((int)countWoman).get(0);
        System.out.println("countMan: " + countMan);
        System.out.println("maxmanheigh: " + maxmanheigh);
        System.out.println("minmanheigh: "+minmanheigh);

        System.out.println("countWoman: "+countWoman);
        System.out.println("maxwomanheigh: " + maxwomanheigh);
        System.out.println("minwomanheigh: "+minwomanheigh);

        sc.close();
    }
}
