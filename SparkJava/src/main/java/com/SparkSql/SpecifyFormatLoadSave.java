package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author medal
 * @create 2019-04-16 10:31
 **/
public class SpecifyFormatLoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SpecifyFormatLoadSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().format("json").load("SparkJava\\src\\main\\java\\com\\SparkSql\\people.json");
        df.select("name").write().format("parquet").save("SparkJava\\src\\main\\java\\com\\SparkSql\\peopleName.parquet");
        df.show();
        sc.close();
    }
}
