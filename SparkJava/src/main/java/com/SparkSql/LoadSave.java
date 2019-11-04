package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author medal
 * @create 2019-04-16 10:21
 **/
public class LoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LoadSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame userDf = sqlContext.read().load("users.parquet");
        userDf.printSchema();
        userDf.show();
        userDf.select("name","favorite_color").write().save("nameAndColors.parquet");
        sc.close();
    }
}
