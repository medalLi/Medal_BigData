package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;


/**
 * @author medal
 * @create 2019-04-16 13:41
 **/
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
        JavaSparkContext sc =  new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        HashMap<String,String> options = new HashMap<String,String>();
        //hm.put("","");
        options.put("url","jdbc:mysql://hadoop.spark.com:3306/bigdata");
        options.put("dbtable","wordCount");
        options.put("user","root");
        options.put("password","centos");

        DataFrame studentInfosDF = sqlContext.read().format("jdbc").options(options).load();
        studentInfosDF.show();
        //options.put("dbtable","student_scores");
        //DataFrame studentScoreDF = sqlContext.read().format("jdbc").options(options).load();
        Row[] rows = studentInfosDF.collect();
        for(Row row : rows){
            System.out.println(row);
        }
    }


}
