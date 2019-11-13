package com.utils;

import com.ly.common.JavaConf;
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
 * Created by ly-lichh on 2018/10/31.
 */
public class HivePool implements Serializable{

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(HivePool.class);
    private static HivePool pool;
    private static ComboPooledDataSource cpds;

    private HivePool(){
    }

    private HivePool(Map<String, String> propertyMap) {
        cpds = new ComboPooledDataSource(true);

        try {
            cpds.setDriverClass(propertyMap.get(JavaConf.HIVE_DRIVER));
            cpds.setJdbcUrl(propertyMap.get(JavaConf.HIVE_JDBC_URL));
            cpds.setUser(propertyMap.get(JavaConf.HIVE_USER));
            cpds.setPassword(propertyMap.get(JavaConf.HIVE_PW));
            cpds.setInitialPoolSize(10);
            cpds.setMinPoolSize(5);
            cpds.setMaxPoolSize(30);
            cpds.setAcquireIncrement(5);
            cpds.setMaxStatements(50);
        } catch (PropertyVetoException e) {
            logger.error(e.getMessage());
        }
    }

    public final static HivePool getInstance(Map<String, String> propertyMap) {

        synchronized(HivePool.class) {
            try {
                if (pool == null) {
                    pool = new HivePool(propertyMap);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        return pool;
    }

    public synchronized final Connection getConnection() {
        Connection conn = null;
        try {
            conn = cpds.getConnection();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }

        return conn;
    }

    public static void release(Connection conn, PreparedStatement pstmt) {
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void releaseStat(Connection conn, Statement stm) {
        if (stm != null){
            try{
                stm.close();
            }catch(Exception e){
                logger.error(e.getMessage());
            }
        }

        if (conn != null){
            try{
                conn.close();
            }catch(Exception e){
                logger.error(e.getMessage());
            }
        }
    }
}
