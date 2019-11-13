package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

/**
 * Spark工具类
 */
public class SparkUtil {

    private static String ZK="";
    private static String PORT = "2181";
    private static String COLUMN_FAMILY;
    private static String TABLE;

    public SparkUtil(String columnFamily,String table,String ZK){
        this.COLUMN_FAMILY=columnFamily;
        this.TABLE=table;
        this.ZK = ZK;
    }

    public  Configuration getConfiguration(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK);
        conf.set("hbase.zookeeper.property.clientPort", PORT);
        conf.set(TableInputFormat.INPUT_TABLE, TABLE);
        return conf;
    }
}
