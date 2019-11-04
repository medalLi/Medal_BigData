package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author medal
 * @create 2019-04-16 10:16
 **/
public class DataFrameOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().json("SparkJava\\src\\main\\java\\com\\SparkSql\\student.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(df.col("name"),df.col("score").plus(1)).show();
        df.filter(df.col("score").gt(80)).show();
        df.groupBy("score").count().show();
        sc.close();
    }
}
