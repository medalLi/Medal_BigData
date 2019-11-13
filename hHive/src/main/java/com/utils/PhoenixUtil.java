package com.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 工具类
 */
public class PhoenixUtil {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args){
//        getConnection();
    }

    /**
     *
     * 获取连接
     * @return
     */
//    public static Connection getConnection(){
//        Connection connect = null;
//        Properties properties = new PropertiesUtil("phoenix.properties").getProperties();
//        String url=properties.getProperty("url");
//        String url = "jdbc:phoenix:xxx,xxx,xxx:2181";
//        try {
//            Properties props = new Properties();
//            props.setProperty("phoenix.query.timeoutMs", "600000");
//            props.setProperty("hbase.rpc.timeout", "600000");
//            props.setProperty("hbase.client.scanner.timeout.period", "600000");
//            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
//            props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.toString(true));
//            PhoenixDriver phoenixDriver = new PhoenixDriver();
//            connect = phoenixDriver.connect(url, props);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return connect;
//    }

    /**
     * 关闭连接
     * @param connection
     */
    public void closeConn(Connection connection){
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
