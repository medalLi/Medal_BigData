package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author medal
 * @create 2019-04-17 14:46
 **/
public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        //创建销售额表，sales表
        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales("
                    +"product String,"+" category String,"+" revenue bigint");
        hiveContext.sql("load data local inpath '' into table sales");

        //先说明一下，row_number()开窗函数，你的作用是什么？
        //其实，就是给每个分组的数据，按照其排序顺序，打上一个分组内的行号
        //比如说，有一个分组date=20160707，里面有3条数据，11211，11212，11213
        //那么对这个分组的每一行使用row_number()开窗函数以后，这个三行会打上一组内的行号
        //行号就是从1开始递增，最后结果就是11211  1，11212  2，11213  3

        DataFrame top3SalesDF = hiveContext.sql("select product,category,revenue" +
                "from ("
                +"product, "
                +"category, "
                +"revenue, "
                +"row_number() over (partition by category order by revenue desc) rank"
                +"from sales "
                +") tmp_sales"
                +"where rank <=3");

        //将每组排名前3的数据，保存到一个表中
        hiveContext.sql("drop table if exists top3_sales");
        top3SalesDF.saveAsTable("top3_sales");

        sc.close();
    }
}
