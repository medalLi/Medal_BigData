package com.hive2hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 从hive读取数据，写入hdfs,再通过定时调度，将数据load到hive表

 */
public class Hive2hdfs2hive {
    // 日志信息
    private static Logger logger = org.slf4j.LoggerFactory.getLogger(Hive2hdfs2hive.class);
    // spark配置信息
    private static String propertyPath = "/user/spark-conf/spark.properties";
    // zookeeper信息
    private static String ZOOKEEPER = "xxx,xxx";
    // Hive jdbc连接信息
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://xxx:10000/库名";
    private static String user = "root";
    private static String password = "";
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    // 结果存放路径
    private static String pathPrefix = "/xxx/xxx";
    private static String resultFile = "part-00000";
    private static String taskName = "xxx";
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
        }

        //从hive中读取数据，使用hive jdbc
//        Connection conn = HivePool.getInstance(propertyMap).getConnection();
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url,user,password);
        String sql = "select * from tableName ";
        String whereSql = " where collect_time_dt = '"+args[0] +"'";
        String execSql = sql+whereSql;
        System.out.println("============ sql ===" + execSql);
        PreparedStatement ps = conn.prepareStatement(sql + whereSql);
        ResultSet rs = ps.executeQuery();
        List<T_Ev_Custom> data = new ArrayList();
        while(rs.next()){
            String xxx1 = rs.getString("xxx1");
            String xxx2 = rs.getString("xxx2");
            String xxx3 = rs.getString("xxx3");

            T t= new T();
            t.setxxx(xxx);

            data.add(t);
        }
        logger.info("一共加载了"+data.size()+"条数据！");
        System.out.println("一共加载了"+data.size()+"条数据！");

        SparkConf sparkConf = new SparkConf().setAppName(Hive2hdfs2hive.class.getSimpleName()+"_"+args[0]);

        sparkConf.registerKryoClasses(new Class[]{T_Ev_Custom.class,ArrayList.class,T.class});
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


        final Broadcast<Map<String, String>> map_bd = javaSparkContext.broadcast(map);
        final Broadcast<String> computeDate_bd = javaSparkContext.broadcast(simpleDateFormat.format(sdf.parse(args[0])));
        JavaRDD<T_Ev_Custom> t_ev_customJavaRDDRDD = javaSparkContext.parallelize(data,10);

        System.out.println("===============data.size===============" + t_ev_customJavaRDDRDD.count());
        /**
         * Rdd转换逻辑
         *
         * */
        System.out.println("==================resultRDD==================size====================== " + resultRDD.count());

        // 将结果进行合并
        resultRDD = resultRDD.coalesce(1,false);

        JavaRDD<String> saveRDD = resultRDD.mapPartitions(new FlatMapFunction<Iterator<T>, String>() {
            @Override
            public Iterator<String> call(Iterator<T> iterator) throws Exception {
                StringBuilder sb = new StringBuilder("");
                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    T amass = iterator.next();
                    sb.append(amass.getxxx()).append("#").append(amass.getxxx()).append("#"));
                    list.add(sb.toString());
                    sb = new StringBuilder("");
                }
                return list.iterator();
            }
        });

        FileSystem fs = HdfsUtils.getFS();
        String computeDate = computeDate_bd.value();
        String path = pathPrefix + "/" + computeDate;
        if (HdfsUtils.ifExists(fs, path)) {
            HdfsUtils.delete_file(fs, path);
        }
        saveRDD.saveAsTextFile(path);
        //对文件重命名
        fs.rename(new Path(path+"/"+resultFile),new Path(path+"/"+taskName+"_"+System.currentTimeMillis() + ".txt"));

        logger.info("=====finish====");
        javaSparkContext.close();
    }

}
