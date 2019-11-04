package com.comment;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;

import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Function MySQL连接通用类
 * CreatedBy ly-yujia on 2017/11/30
 * ModifiedBy ly-yujia on 2017/11/30
 */
public class MysqlPool implements Serializable {

  //  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MysqlPool.class);
    private static MysqlPool pool;
    private static ComboPooledDataSource cpds;


    private MysqlPool() {
    }

    private MysqlPool(Map<String, String> propertyMap) {
        cpds = new ComboPooledDataSource(true);
//        System.out.println("cpds : "+cpds);
//        System.out.println(propertyMap.get(Conf.MYSQL_URL));
//        System.out.println(propertyMap.get(Conf.MYSQL_USRENAME));
//        System.out.println(propertyMap.get(Conf.MYSQL_PW));
        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
            cpds.setJdbcUrl(propertyMap.get(Conf.MYSQL_URL));
            cpds.setUser(propertyMap.get(Conf.MYSQL_USRENAME));
            cpds.setPassword(propertyMap.get(Conf.MYSQL_PW));
            //cpds.setInitialPoolSize(5);
            ///cpds.setMinPoolSize(3);
           // cpds.setMaxPoolSize(10);
            cpds.setInitialPoolSize(1);
            cpds.setMinPoolSize(1);
            cpds.setMaxPoolSize(1);
            cpds.setAcquireIncrement(5);
            cpds.setMaxStatements(50);
            cpds.setMaxIdleTime(3600);
           // cpds.setMaxIdleTime(7200);
            cpds.setIdleConnectionTestPeriod(120);
           // cpds.setIdleConnectionTestPeriod(1200);
        } catch (PropertyVetoException e) {
            //logger.error(e.getMessage());
            System.out.println("配置mysqlPool属性出错了！！！");
        }
        //System.out.println(cpds.getUser() + "--" + cpds.getJdbcUrl()+"--"+cpds.getPassword());
    }

    // 传入mysql 配置信息，获取mysqlPool实例
    public final static MysqlPool getInstance(Map<String, String> propertyMap) {

        synchronized(MysqlPool.class) {
            try {
                if (pool == null) {
                    pool = new MysqlPool(propertyMap);
                }
            } catch (Exception e) {
                //logger.error(e.getMessage());
                System.out.println("获取mysql实例出错啦！！！");
            }
        }
        //System.out.println("pool : " + pool);
        return pool;
    }

    public synchronized final Connection getConnection() {
        Connection conn = null;
        try {
            conn = cpds.getConnection();
        } catch (SQLException e) {
           // logger.error(e.getMessage());
            System.out.println("获取conn出错了！！！");
        }

        return conn;
    }

    public static void release(Connection conn, PreparedStatement pstmt) {
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (Exception e) {
               // logger.error(e.getMessage());
                System.out.println("关闭pstmt 对象出错了！！！");
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                //logger.error(e.getMessage());
                System.out.println("关闭conn 对象出错了！！！");
            }
        }
    }

    public static void releaseStat(Connection conn, Statement stm) {
        if (stm != null){
            try{
                stm.close();
            }catch(Exception e){
                //logger.error(e.getMessage());
                System.out.println("关闭stm 对象出错了！！！");
            }
        }

        if (conn != null){
            try{
                conn.close();
            }catch(Exception e){
               // logger.error(e.getMessage());
                System.out.println("关闭conn 对象出错了！！！");
            }
        }
    }


}