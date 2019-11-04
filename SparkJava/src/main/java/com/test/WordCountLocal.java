package com.test;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCountLocal {
	/**
	 * 本地测试wordcount程序
	 * 
	 * */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		JavaRDD<String> lines = sc.textFile("E:\\学习资料\\代码测试数据\\test3.txt");

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>(){
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(line.split("\t"));
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

		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>(){
			private static final long seriaVersionUID = 1L;
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(wordCount._1 + " append "+ wordCount._2 + " times.");
			}		
		});
		
		sc.close();
	}

}
