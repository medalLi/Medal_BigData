package com.hive2hdfs;

import com.ly.bean.input.T_Ev_Custom;
import com.ly.bean.output.T_Dw_Pviov_Chrg_Amass;
import com.ly.common.JavaConf;
import com.ly.common.MyConstants;
import com.ly.utils.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

/**

 * 从hive读取数据，写入hbase

 */
public class Hive2hbase {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(Hive2hbase.class);
    private static String propertyPath = "/user/spark-conf/spark.properties";
    private static String ZOOKEEPER = "iov-hadoop-01,iov-hadoop-02,iov-hadoop-03,iov-hadoop-04,iov-hadoop-05";
//    private static String ZOOKEEPER = "hadoop02,hadoop03,hadoop04,hadoop05,hadoop06";
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://iov-hadoop-01:10000/pv_ev";
    private static String user = "root";
    private static String password = "";
    private static String tableName = "PV_EV:T_DW_PVIOV_CHRG_AMASS";
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    public static void main(String[] args)throws Exception{

//        long lastTimeStamp = 0;
//        long dateTimeStamp = 0;
//        Map<String, String> propertyMap = new HashMap<>();
//        propertyMap.put(JavaConf.HBASE_ZK_QUORUM,ZOOKEEPER);
        if(args.length != 0){
            try {

                System.out.println("=====传入的日期格式需为：yyyy-MM-dd");
                sdf.parse(args[0]);
                System.out.println(args[0]);
//                if(args[1] == null){
//                    throw new Exception("请传入hdfs上的配置文件路径前缀，如: \"hdfs://hadoop01:8020\"");
//                }
//                propertyPath = args[1] + propertyPath;
            }catch (Exception e){
                logger.error(e.getMessage());
                System.exit(1);
            }
        }else {
//            dateTimeStamp = DateUtils.getDateTimeStamp();
//            lastTimeStamp = DateUtils.getLastTimeStamp();
//            propertyPath = "hdfs://iov-hadoop-01:8020" + propertyPath;
//            propertyPath = "hdfs://hadoop01:8020" + propertyPath;
        }
        
        

//        propertyMap = PropertyUtils.getPropertyFromHDFS(propertyPath);
//        propertyMap.put("hive.driver","hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181,hadoop06:2181");
//        propertyMap.put("hive.driver","org.apache.hive.jdbc.HiveDriver");
//        propertyMap.put("hbase.zookeeper.quorum","hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181,hadoop06:2181");
//        propertyMap.put("hive.user","root");
//        propertyMap.put("hive.pw","123456");


        //从hive中读取数据，使用hive jdbc
//        Connection conn = HivePool.getInstance(propertyMap).getConnection();
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url,user,password);
        String sql = "select vin_code as vinCode,collect_time as collectTime ,upload_time as uploadTime," +
                "charge,charge_serial_no from pv_ev.t_ev_custom_data ";
        String whereSql = " where collect_time_dt = '"+args[0] +"'";
        String execSql = sql+whereSql;
        System.out.println("============ sql ===" + execSql);
        PreparedStatement ps = conn.prepareStatement(sql + whereSql);
        ResultSet rs = ps.executeQuery();
        List<T> data = new ArrayList();
        while(rs.next()){
            String xxx = rs.getString(1);
            T t = new T();
            t.setXxx(xxx);
            data.add(t);
        }
        logger.info("一共加载了"+data.size()+"条数据！");
        System.out.println("一共加载了"+data.size()+"条数据！");

        SparkConf sparkConf = new SparkConf().setAppName("T_Ev_Custom_Data");

        sparkConf.registerKryoClasses(new Class[]{T_Ev_Custom.class,ArrayList.class,T_Dw_Pviov_Chrg_Amass.class});
//        if(args.length == 2 ){
//            sparkConf.setMaster("local[*]");
//        }
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        javaSparkContext.setLogLevel("INFO");
        javaSparkContext.hadoopConfiguration().set("mapred.job.reuse.jvm.num.tasks","2000");
        javaSparkContext.hadoopConfiguration().set("mapreduce.job.reduce.slowstart.completedmaps","0.8");

        Map<String,String> map = new HashMap<String, String>();
        map.put(MyConstants.SLOW_CHARGE_BEGIN,MyConstants.SLOW_CHARGE_END);
        map.put(MyConstants.QUICK_CHARGE_BEGIN,MyConstants.QUICK_CHARGE_END);


//        final Broadcast<Map<String, String>> broadcast = javaSparkContext.broadcast(propertyMap);
        final Broadcast<String> tableName_BD = javaSparkContext.broadcast(tableName);
        final Broadcast<Map<String, String>> map_bd = javaSparkContext.broadcast(map);

        JavaRDD<T> t_ev_customJavaRDDRDD = javaSparkContext.parallelize(data,10);

        /*
        * RDD转换逻辑
        * */

        // 缓存数据
        vinAndListRDD = vinAndListRDD.persist(StorageLevel.DISK_ONLY());

        System.out.println("==================resultRDD==================size====================== " + resultRDD.count());
//        System.out.println("==================resultRDD===================================== " + resultRDD.collect().toString());
        resultRDD = resultRDD.repartition(10);
        resultRDD.foreachPartition(new VoidFunction<Iterator<T>>() {
            @Override
            public void call(Iterator<T> iterator) throws Exception {
                String tablename = tableName_BD.value();
                String family = "CF";
                Configuration configuration = HBaseConfiguration.create();
                configuration.set(JavaConf.HBASE_ZK_QUORUM,ZOOKEEPER);
                configuration.set(JavaConf.HBASE_ZK_PORT, "2181");
                org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(configuration);
                HTable hTable = (HTable)conn.getTable(TableName.valueOf(tablename));
                hTable.setAutoFlushTo(false);
                hTable.setWriteBufferSize(2*1024*1024);
                String strtDate = "STAT_DATE";
                String vin = "VIN";
                while(iterator.hasNext()){
                    T dw = iterator.next();
                    Put put = new Put(Bytes.toBytes(dw.getChargeStartTime()+dw.getVin()));
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(strtDate),Bytes.toBytes(dw.getStartDate() == null ? "":dw.getStartDate()));
                    hTable.put(put);
                }
                hTable.flushCommits();
                hTable.close();
            }
        });
        logger.info("=====finish====");
        javaSparkContext.close();
    }
}
