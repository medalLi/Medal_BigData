package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author medal
 * @create 2019-04-16 10:09
 **/
public class DataFrameCreate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameCreate").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().json("SparkJava\\src\\main\\java\\com\\SparkSql\\student.json");
        df.show();
        sc.close();
    }
}
