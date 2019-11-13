package com.hive2hdfs.Hbase2mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
用phonix查hbase表，并将数据插入mysql表中
 */
public class Demo01 {
    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(Demo01.class);
    //    private static String properyPath = "hdfs://172.26.171.58:8020/user/spark-conf/spark.properties";
    // phoenix_url,xxx为主机名
    private static final String DB_PHOENIX_URL = "jdbc:phoenix:xxx,xxx,xxx:2181";
    // zookeeper的地址
    private static final String HBASE_ZK_QUORUM = "xxx:2181,xxx:2181,xxx:2181";
    // mysql的基本设置
    private static final String MYSQL_USER = "jdbc.username";
    private static final String MYSQL_PASSWORD = "jdbc.password";
    private static final String INIT_POOL_SIZE = "init.pool.size";
    private static final String MIN_POOL_SIZE = "min.pool.size";
    private static final String MAX_POOL_SIZE = "max.pool.size";
    // 目的表名
    private static final String SAVE_TABLE_NAME = "xxx";

    private long startTime;
    private long endTime;

    public Demo01(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }
    public void run(List<Foot> footList, List<Point> pointList) {
        Statement stmt = null;
        Connection conn = null;
//        conn= PhoenixUtil.getConnection();
        try {
            stmt=conn.createStatement();
            String sql = "select * from tableName ";
            ResultSet rs = stmt.executeQuery(sql);
            //a:从HBASE取数据
            while (rs.next()) {
                if (rs.getString(1) == null) {
                    continue;
                } else {
                    String xxx= rs.getString(1);

                    T foot = new T();
                    foot.setXxx(xxx);
                    footList.add(foot);
                }
            }
            rs.close();
            stmt.close();
            conn.close();

            Connection conn_gps = null;
            Statement stmt_gps=conn_gps.createStatement();

                String sql_gps = " select * from tableName";
                ResultSet rs_gps = stmt_gps.executeQuery(sql_gps);
                while (rs_gps.next()) {
                    String xxx= rs_gps.getString(1);
                    String xxx = rs_gps.getString(2);
                    if ("0".equals(sorn) && "0".equals(rorw) ) {
                        T point = new T();
                        String lon = rs_gps.getString(1);
                        pointList.add(point);
                    }
                }
            stmt_gps.close();
            conn_gps.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("===========footList============== " +footList.size());
        System.out.println("===========pointList============== " +pointList.size());
    }
}
