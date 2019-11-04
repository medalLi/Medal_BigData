package com.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.collection.mutable.ArraySeq;

public class WordCountCluster {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//SparkConf conf = new SparkConf().setAppName("simple Application").setMaster("spark://hadoop.spark.com:7077");
		//远程调试

		SparkConf conf = new SparkConf().setAppName("simple Application").
				setMaster("spark://hadoop.spark.com:7077").
				setJars(new String[]{"E:\\WorkSpaces\\Workspace--idea\\BigData\\SparkJava\\target\\SparkJava-1.0-SNAPSHOT.jar"});



		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//JavaRDD<String> lines = sc.textFile("hdfs://hadoop.medal.com:8020/user/xunzhang/mapreduce/wordcount/input/wc.input",1);
		JavaRDD<String> lines = sc.textFile("hdfs://hadoop.spark.com:8020//user/medal/mapreduceTest/phone_data.txt",1);
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>(){
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(line.split(" "));
			}			
		});
		
		JavaPairRDD<String,Integer> pairs = words.mapToPair(
				new PairFunction<String,String,Integer>(){
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String word) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String,Integer>(word,1);
					}				
				});
		JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer,Integer,Integer>(){
					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}				
				});

		wordCounts.saveAsTextFile("hdfs://hadoop.spark.com:8020//user/medal/mapreduceTest/yuancheng");
//		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>(){
//			private static final long seriaVersionUID = 1L;
//			public void call(Tuple2<String, Integer> wordCount) throws Exception {
//				// TODO Auto-generated method stub
//				System.out.println(wordCount._1 + " append "+ wordCount._2 + " times.");
//			}
//		});
		
		sc.close();
	}

}
