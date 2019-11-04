package com.SparkSql;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.*;


import java.util.ArrayList;
import java.util.List;

/**
 * @author medal
 * @create 2019-04-16 11:18
 **/
public class JsonDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JsonDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("");

        //针对学生成绩信息的DataFrame,注册临时表，查询分数大于80分的学生的姓名
        df.registerTempTable("student_scores");
        DataFrame goodStudentNameDF = sqlContext.sql("select name,score from student_scores");
        //我们接下来把它给转换一下，因为这个时候DataFrame里面的元素还是Row，我们将其转成String
        List<String> goodStudentNames = goodStudentNameDF.toJavaRDD().map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        List<String> studentInfoJsons = new ArrayList<String>();
        studentInfoJsons.add("{\"name\":\"Yasaka\",\"age\":18}");
        studentInfoJsons.add("{\"name\":\"Xuruyun\",\"age\":17}");
        studentInfoJsons.add("{\"name\":\"Liangyongqi\",\"age\":19}");

        JavaRDD<String> studentInfosRDD = sc.parallelize(studentInfoJsons);
        DataFrame df1 = sqlContext.read().json(studentInfosRDD);
        df.registerTempTable("student_infos");
        String sql = "select name,age from student_infos where name (";
        for (int i = 0; i < goodStudentNames.size() ; i++) {
            sql += "'"+goodStudentNames.get(i)+"'";
            if(i < goodStudentNames.size() -1){
                sql += ",";
            }
        }
        sql +=")";
        System.out.println(sql);

        DataFrame goodStudentInfoDF = sqlContext.sql(sql);

//        //然后将两份数据的DataFrame执行Join算子操作
//        JavaPairRDD<String, Tuple2<Integer,Integer>> goodStudentsRDD = goodStudentInfoDF.javaRDD().map
//                mapToPair(new PairFunction<Row, String, Tuple2<Integer, Integer>>() {
//            public Tuple2<Integer, Integer>> call(Row row) throws Exception {
//                return new Tuple2<String, Tuple2<Integer, Integer>>(String.valueOf(row.get(0),Integer.valueOf(row.getString(1))));
//            }
//        }).join(df.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
//            public Tuple2<String, Integer> call(Row row) throws Exception {
//                return new Tuple2<String, Integer>(String.valueOf(row.get(0),Integer.valueOf(row.get(1))));
//            }
//        }));
//
//        JavaRDD<Row> goodStudentsdRowRDD = goodStudentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
//            public Row call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
//                return RowFactory.create(stringTuple2Tuple2._1,stringTuple2Tuple2._2._1,stringTuple2Tuple2._2._2);
//            }
//        });
//
//        List<StructField> fields = new ArrayList<StructField>();
//        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
//        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
//        fields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
//
//        StructType structType = DataTypes.createStructType(fields);
//        DataFrame goodStudentDF = sqlContext.createDataFrame(goodStudentsdRowRDD,structType) ;
//        goodStudentDF.write().format("json").mode(SaveMode.Overwrite).save("goodStudentJson");
    }
}
