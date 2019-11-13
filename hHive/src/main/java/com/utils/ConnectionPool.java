package com.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 */
public class ConnectionPool {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private volatile static ConnectionPool instance;
     private static ComboPooledDataSource dataSource;
    public static ConnectionPool getInstance() {
        if (instance == null) {
            synchronized (ConnectionPool.class) {
                if (instance == null) {
                    instance = new ConnectionPool();
                }
            }
        }
        return instance;
    }


    private ConnectionPool() {
//        Properties properties = new PropertiesUtil("jdbc.properties").getProperties();
        try {
//            String driverClass = properties.getProperty("jdbc.driverClassName");
//            String jdbcUrl = properties.getProperty("jdbc.url");
//            String user = properties.getProperty("jdbc.username");
//            String password = properties.getProperty("jdbc.password");
            String driverClass = "com.mysql.jdbc.Driver";
            String jdbcUrl = "jdbc:mysql://xxx:3306/IOV?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
            String user = "xxx";
            String password = "xxx";
//            int initPoolSize = Integer.parseInt(properties.getProperty("init.pool.size"));
//            int minPoolSize = Integer.parseInt(properties.getProperty("min.pool.size"));
//            int maxPoolSize = Integer.parseInt(properties.getProperty("max.pool.size"));
            int initPoolSize = 10;
            int minPoolSize = 5;
            int maxPoolSize = 20;
            dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass(driverClass);
            dataSource.setJdbcUrl(jdbcUrl);
            dataSource.setUser(user);
            dataSource.setPassword(password);
            dataSource.setInitialPoolSize(initPoolSize);
            dataSource.setMinPoolSize(minPoolSize);
            dataSource.setMaxPoolSize(maxPoolSize);
        } catch (Exception e) {
            log.error("Exception:", e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public static void destroy() throws SQLException {
        DataSources.destroy(dataSource);
    }

}
