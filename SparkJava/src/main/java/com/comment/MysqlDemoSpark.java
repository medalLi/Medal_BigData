package com.comment;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MysqlDemoSpark {
    public static void main(String[] args) {
        //读取配置文件
        InputStream is = MysqlDemoSpark.class.getClassLoader().getResourceAsStream("mysql.properties");
        Map<String, String> mysqlMap = PropertiesUtils.get_MYSQL_PROP(is);

        SparkConf conf = new SparkConf().setAppName("MysqlDemoSpark").setMaster("spark://hadoop.spark.com:7077").
                setJars(new String[]{"E:\\WorkSpaces\\Workspace--idea\\BigData\\SparkJava\\target\\SparkJava-1.0-SNAPSHOT.jar"});

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile("hdfs://hadoop.spark.com:8020//user/medal/ba/", 10000);

        lines.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Bean> words = lines.map(new Function<String, Bean>() {
            @Override
            public Bean call(String v1) throws Exception {
                String[] lineSplit = v1.split("\t");

                return new Bean(lineSplit[0],
                        lineSplit[1], lineSplit[2], lineSplit[3],
                        lineSplit[4], lineSplit[5], lineSplit[6],
                        lineSplit[7], lineSplit[8], lineSplit[9]
                );
            }
        });

        words.foreachPartition(new VoidFunction<Iterator<Bean>>() {
            @Override
            public void call(Iterator<Bean> beanIterator) throws Exception {

                Connection conn = MysqlPool.getInstance(mysqlMap).getConnection();
                // System.out.println("conn : " +conn);

                try {
                    conn.setAutoCommit(false);
                    //ArrayList<String> list = new ArrayList<>();
                    //ArrayList<Bean> list = ReadFile("D://000000_0");
                    StringBuffer sb = new StringBuffer("");

                    String sql = "insert into mysql_batch" +
                            " (ota_bu_behavior_da_id,da_id,event_id,event_value," +
                            "creator,created_date,modifier,last_updated_date,car_type,pt_time) values";
                    PreparedStatement ps = conn.prepareStatement(sql);
                    while (beanIterator.hasNext()) {
                        Bean bean = beanIterator.next();
                        sb.append("('" + bean.getOta_bu_behavior_da_id() + "','" + bean.getDa_id() + "','" +
                                bean.getEvent_id() + "','" + bean.getEvent_value() + "','" +
                                bean.getCreator() + "','" + bean.getCreated_date() + "','" +
                                bean.getModifier() + "','" + bean.getLast_updated_date() + "','" +
                                bean.getCar_type() + "','" + bean.getPt_time() + "'),");
                    }

                    String exe_sql = sql + sb.substring(0, sb.length() - 1);
                    //Sys  tem.out.println(list.size());
                    //System.out.println("exe_sql : " +exe_sql);
                    long startTime = System.currentTimeMillis();
                    ps.addBatch(exe_sql);
                    try {
                        ps.executeBatch();
                    } catch (Exception e) {
                        e.printStackTrace();
                        //批插失败，单插
                        conn.close();
                        conn = MysqlPool.getInstance(mysqlMap).getConnection();
                        conn.setAutoCommit(true);
                        String insert_one_sql = "insert into mysql_batch" + " (ota_bu_behavior_da_id,da_id,event_id,event_value," +
                                "creator,created_date,modifier,last_updated_date,car_type,pt_time) values (?,?,?,?,?,?,?,?,?,?)";
                        PreparedStatement one_ps = conn.prepareStatement(insert_one_sql);
                        //   System.out.println("insert_one_sql : " + insert_one_sql);

                        while (beanIterator.hasNext()) {
                            Bean bean = beanIterator.next();
                            try {
                                System.out.println("执行单插！！！");
                                insertData(bean, one_ps);
                            } catch (Exception e1) {
                                //logger.error("==============" + e1.getMessage());
                                e1.printStackTrace();
                            }
                        }

                    }
                    conn.commit();
                    ps.close();
                    long end = System.currentTimeMillis();
                    System.out.println("共耗时： " + (end - startTime));
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });


        sc.close();


    }


    /**
     * 往数据库插入数据,来一条数据执行一次
     */
    private static void insertData(Bean bean, PreparedStatement ps) throws Exception {
        ps.setString(1, bean.getOta_bu_behavior_da_id());
        ps.setString(2, bean.getDa_id());
        ps.setString(3, bean.getEvent_id());
        ps.setString(4, bean.getEvent_value());
        ps.setString(5, bean.getCreator());
        ps.setString(6, bean.getCreated_date());
        ps.setString(7, bean.getModifier());
        ps.setString(8, bean.getLast_updated_date());
        ps.setString(9, bean.getCar_type());
        ps.setString(10, bean.getPt_time());
        ps.executeUpdate();
    }

    public static ArrayList<Bean> ReadFile(String fileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        ArrayList<Bean> list = new ArrayList<>();
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] lineSplit = line.split("\t");
            list.add(new Bean(lineSplit[0],
                    lineSplit[1], lineSplit[2], lineSplit[3],
                    lineSplit[4], lineSplit[5], lineSplit[6],
                    lineSplit[7], lineSplit[8], lineSplit[9]
            ));
            if (list.size() == 10000) {
                break;
            }
        }

        return list;
    }

//    public static void main(String[] args) throws IOException {
//        ArrayList<Bean> list = ReadFile("D://000000_0");
//        for (int i = 0; i < 10; i++) {
//            System.out.println(list.get(i));
//        }
//
//        System.out.println(list.size());
//    }
}

