package com.sql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author medal
  * @create 2019-04-15 22:14
  **/
object demo01 {
  def main(args:Array[String]){
    //val conf = new SparkConf().setAppName("LocalFile").setMaster("local")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("E:\\学习资料\\代码测试数据\\test3.txt",1)
    //val lines = sc.textFile("hdfs://hadoop.spark.com:8020/user/medal/mapreduceTest/fruit.tsv", 1)

    val count = lines.map{line => line.length()}.reduce(_ + _)

    println("file's count is "+count)
  }
}
