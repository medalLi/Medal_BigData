package com.SparkSql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author medal
 * @create 2019-04-16 11:03
 **/
public class RDD2DataFrameDynamic {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameDynamic").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("SparkJava\\src\\main\\java\\com\\SparkSql\\student.txt");

        JavaRDD<Row> rows = lines.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] lineSplit = line.split(",");

                return RowFactory.create(Integer.valueOf(lineSplit[0]),lineSplit[1],Integer.valueOf(lineSplit[2]));
            }
        });

        //动态构造元数据，还有一种方式是通过反射的方式来构建DataFrame，这里我们用的是动态创建元数据
        //有些时候我们一开始不确定有哪些列，而这些列需要从数据库比如mysql或者配置文件来加载
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        StructType schema = DataTypes.createStructType(fields);
        DataFrame studentDF = sqlContext.createDataFrame(rows,schema);
        studentDF.registerTempTable("stu");

        DataFrame teenagerDF = sqlContext.sql("select * from stu where age <= 18");

        List<Row> studentList = teenagerDF.javaRDD().collect();
        for(Row row : studentList){
            System.out.println(row);
        }
    }
}
