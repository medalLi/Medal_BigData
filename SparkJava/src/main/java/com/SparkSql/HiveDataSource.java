package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author medal
 * @create 2019-04-16 16:11
 **/
/*
* 1.把hive里面的hive-site.xml放到spark/conf/目录下
* 2.启动Hive
* 3.首先启动mysql
* 4.启动HDFS
* 5.初始化HiveContext
* 6.打包运行
*
* 如果你所在的客户端没有把hive-site.xml发送到每一个Spark所在的conf目录下的话，就必须--file ./conf/hive-site.xml
*
* Found both spark.executor.extraClassPath and SPARD_CLASSPATH. Use only the former.
* 出现这个错误，你就把spark-env.sh里面的注释掉
* */
public class HiveDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HiveDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);

        //判断是否存储过student_infos这张表，如果存储过则删除
        hiveContext.sql("drop table if exists movie_info1");
        //重建
        hiveContext.sql("create table movie_info1(" +
                "movie string, " +
                "category array<string>) " +
                "row format delimited fields terminated by \"\t\" " +
                "collection items terminated by \",\"");
        //加载数据
        hiveContext.sql("load data local inpath '/newDisk/test/myProject/youtobe/movie_info.tsv' into table movie_info1");

        //一样的方式导入其它表
//        hiveContext.sql("drop table if exists student_scores");
//        hiveContext.sql("create table if not exists student_scores (name String,score int)");
//        hiveContext.sql("load data...");
//
//        //关联两张表，查询成绩大于80分的学生
//        DataFrame goodStudentsDF = hiveContext.sql("select si.name,si.age,ss.score " +
//                "from student_infos si join .......");
//        //我们得到这个数据再存回hive表中
//        hiveContext.sql("drop table if exists good_student_infos");
//        goodStudentsDF.saveAsTable("good_student_infos");

        //将hive表中的数据读取出来
        DataFrame temp = hiveContext.table("person_info");
        Row[] rows = temp.collect();
        for(Row row : rows){
            System.out.println(row);
        }

        sc.close();
    }
}
