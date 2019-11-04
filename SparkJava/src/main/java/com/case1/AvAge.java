package com.case1;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;

/**
 * @author medal
 * @create 2019-04-09 17:38
 **/
/*
* 统计一个1000万人口的所有人的平均年龄
* */
public class AvAge {
//    static {
//        MokeData.createData(3);
//    }
    public static void main(String[] args) {
        //System.out.println("hello");
        SparkConf conf = new SparkConf().setAppName("AvAge").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile("SparkJava" +
                "\\src\\main\\java\\com\\case1\\data.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>(){
            private static final long serialVersionUID = 1L;

            public Iterable<String> call(String line) throws Exception {
                // TODO Auto-generated method stub
                return Arrays.asList(line.split("\t")[1]);
            }
        });

        final Accumulator<Integer> sum = sc.accumulator(0);

        words.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                sum.add(Integer.parseInt(s));
            }
        });

       System.out.println(1.0*sum.value()/3);


        sc.close();
    }
}
