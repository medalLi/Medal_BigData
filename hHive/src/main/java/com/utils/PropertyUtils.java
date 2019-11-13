package com.utils;

import com.ly.common.JavaConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 */
public class PropertyUtils {

    public static Properties getProperties(String filePath){
        Properties properties = new Properties();
        try{
            FileInputStream  fis = new FileInputStream(filePath);
            properties.load(fis);
        }catch (Exception e){
            e.printStackTrace();
        }
        return properties;
    }


    /**
     * 读取本地配置文件信息
     * @return
     */
    public static Map<String, String> getLocalMapProperties(String filePath){
        Properties properties = getProperties(filePath);
        Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put(JavaConf.HIVE_DRIVER,properties.getProperty(JavaConf.HIVE_DRIVER));
        propertyMap.put(JavaConf.HIVE_JDBC_URL,properties.getProperty(JavaConf.HIVE_JDBC_URL));
        propertyMap.put(JavaConf.HIVE_USER,properties.getProperty(JavaConf.HIVE_USER));
        propertyMap.put(JavaConf.HIVE_PW,properties.getProperty(JavaConf.HIVE_PW));
        return propertyMap;
    }
    /**
     *  读取hdfs上的配置文件信息
     * @return
     */
    public static Map<String, String> getPropertyFromHDFS(String path){
        Map<String, String> propertyMap = new HashMap<>();
       try{
           Configuration configuration = new Configuration();
           FileSystem fs = FileSystem.get(configuration);
           FSDataInputStream is = fs.open(new Path(path));
           Properties properties = new Properties();
           properties.load(is);
           propertyMap.put(JavaConf.HIVE_DRIVER,properties.getProperty(JavaConf.HIVE_DRIVER));
           propertyMap.put(JavaConf.HIVE_JDBC_URL,properties.getProperty(JavaConf.HIVE_JDBC_URL));
           propertyMap.put(JavaConf.HIVE_USER,properties.getProperty(JavaConf.HIVE_USER));
           propertyMap.put(JavaConf.HIVE_PW,properties.getProperty(JavaConf.HIVE_PW));
           propertyMap.put(JavaConf.HBASE_ZK_QUORUM,properties.getProperty(JavaConf.HBASE_ZK_QUORUM));

       }catch (Exception e){
            e.printStackTrace();
       }
        return propertyMap;
    }

    public static void main(String[] args)throws Exception{
       System.out.println(getPropertyFromHDFS("hdfs://hostName:8020/user/spark-conf/spark.properties"));
//       System.out.println(getPropertyFromHDFS("/user/spark-conf/spark.properties"));
    }

}
