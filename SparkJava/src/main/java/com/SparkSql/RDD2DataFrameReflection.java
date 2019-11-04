package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.codehaus.janino.Java;

import java.util.List;

/**
 * @author medal
 * @create 2019-04-16 10:41
 **/
public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("SparkJava\\src\\main\\java\\com\\SparkSql\\student.txt");
        JavaRDD<Student> studentRDD = lines.map(new Function<String, Student>() {
            public Student call(String line) throws Exception {
                String[] lineSplit = line.split(",");
                Student stu = new Student();
                stu.setId(Integer.valueOf(lineSplit[0]));
                stu.setName(lineSplit[1]);
                stu.setAge(Integer.valueOf(lineSplit[2]));
                return stu;
            }
        });

        //使用反射方式将RDD转换成DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(studentRDD,Student.class);
        studentDF.printSchema();
        //有了DataFrame后就可以注册一个临时表，SQL语句还是查询年龄小于18的人
        studentDF.registerTempTable("student");

        DataFrame teenagerDF = sqlContext.sql("select * from student where age <= 18");

        JavaRDD<Row> teenagerRDD = teenagerDF.toJavaRDD();
        JavaRDD<Student> teeagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            public Student call(Row row) throws Exception {
                int id = row.getAs("id");
                int age = row.getAs("age");
                String name = row.getAs("name");
                Student stu = new Student();
                stu.setId(id);
                stu.setName(name);
                stu.setAge(age);
                return stu;
            }
        });

        List<Student> studentList = teeagerStudentRDD.collect();
        for(Student stu : studentList){
            System.out.println(stu);
        }
    }
}
