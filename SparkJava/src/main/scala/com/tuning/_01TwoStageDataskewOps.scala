package com.tuning

import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author medal
  * @create 2019-07-20 21:56
  **/
// 两阶段聚合操作
object _01TwoStageDataskewOps {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")

    val sc = new SparkContext(conf)

    val list = List("hello hello hello hello you you hello",
      "hello hello hello you you hei hei hello hello hello")

    val listRDD = sc.parallelize(list)

    val pairsRDD:RDD[(String,Int)] = listRDD.flatMap(line =>{
      line.split(" ")
    }).map((_,1))

    // 1.确认发生数据倾斜的key   --->  sample 算子
    val sorted =pairsRDD.sample(true,0.6).countByKey().toList.sortWith((m1,m2) => m1._2 > m2._2)

    println("抽样数据排序之后的结果：")
    println(sorted.mkString("\n"))

    val dataskewKey = sorted.head._1
    println("发生数据倾斜的key为："+ dataskewKey)
    //2.加随机前缀
    val prefixPairsRDD = pairsRDD.map{case (word,count) => {
      if(word == dataskewKey){
        val random = new Random()
        val prefix = random.nextInt(2)
        (s"${prefix}_${word}",count)
      } else{
        (word,count)
      }
    }}
    println("=========== 2.加随机前缀数据 =================")
    prefixPairsRDD.foreach(println)

    //3.局部聚合
    val partAggrRDD:RDD[(String,Int)] = prefixPairsRDD.reduceByKey(_+_)
    println("=========== 3.局部聚合 =================")
    partAggrRDD.foreach(println)
    //4.去掉随机前缀
    val unPrefixPairsRDD = partAggrRDD.map{case (word,count) => {
      if (word.contains("_")){
        (word.substring(2),count)
      }else{
        (word,count)
      }
    }}
    println("=========== 4.去掉随机前缀 =================")
    unPrefixPairsRDD.foreach(println)
    //5.进行全局聚合
    val fullAggrRDD = unPrefixPairsRDD.reduceByKey((_+_))
    println("=========== 5.进行全局聚合 =================")
    fullAggrRDD.foreach(println)

    sc.stop()
  }

}
