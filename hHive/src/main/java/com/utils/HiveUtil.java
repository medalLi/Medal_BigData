package com.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Hive工具类
 * Created by ly-zhaiqian on 2018/9/13.
 */
public class HiveUtil {

    //hive的jdbc驱动类
    public static String dirverName = "org.apache.hive.jdbc.HiveDriver";
    //连接hive的URL hive1.2.1版本需要的是jdbc:hive2，而不是 jdbc:hive
    //public static String url = "jdbc:hive2://ly-hadoop-01:10000/default";
    public static String url = "jdbc:hive2://hostName:10000/default";
    //登录linux的用户名  一般会给权限大一点的用户，否则无法进行事务形操作
    public static String user = "xxx";
    //登录linux的密码
    public static String pass = "xxx";

    static final Logger logger = LoggerFactory.getLogger(HiveUtil.class);
    /**
     * 创建连接
     * @return
     * @throws SQLException
     */
    public static Connection getConn(){
        Connection conn = null;
        try {
            Class.forName(dirverName);
            conn = DriverManager.getConnection(url, user, pass);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 创建命令
     * @param conn
     * @return
     * @throws SQLException
     */
    public static Statement getStmt(Connection conn) throws SQLException{
        if(conn == null){
            logger.debug("this conn is null");
        }
        return conn.createStatement();
    }

    /**
     * 关闭连接
     * @param conn
     */
    public static void closeConn(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 关闭命令
     * @param stmt
     */
    public static void closeStmt(Statement stmt){
        try {
            stmt.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 建表
     * @param stmt
     * @param sql
     */
    public static void createTable(Statement stmt,String sql){
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //添加数据
    public static void insert(Statement ps) throws SQLException {
        String sql = "load data inpath ' hdfs://hostName:8020/usr/hive/goods2.txt' into table goods2";
        //记得先在文件系统中上传goods.txt
        ps.execute(sql);
    }
}
