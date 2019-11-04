package MyExamples.pracities1_7;

/**
 * @author medal
 * @create 2019-03-30 15:13
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class HBaseDao{
    private int regions;
    private String namespace;
    private String tableName;
    public static final Configuration conf;
    private Table table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    static {
        conf = HBaseConfiguration.create();
    }

    public HBaseDao() {
        try {
            //regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
            namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
            // System.out.println("3  ！！！");
            connection = ConnectionFactory.createConnection(conf);

           // HBaseUtil.initNamespace(conf, namespace);
            //HBaseUtil.createTable(conf, tableName, "f1");
            // }

            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761
     * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761
     * HBase表的列：call1  call2   build_time   build_time_ts   flag   duration
     * @param ori
     */
    public void put(String ori) {
        try {
            String[] splitOri = ori.split("\t");

            String word = splitOri[0];
            String count = splitOri[1];

            //生成rowkey
           // String rowkey = HBaseUtil.genRowKey(regionCode, caller, buildTimeReplace, callee, "1", duration);
            String rowkey ="1001";
            //向表中插入该条数据
            Put put1 = new Put(Bytes.toBytes(rowkey));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("word"), Bytes.toBytes(word));
            put1.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("count"), Bytes.toBytes(count));

            table.put(put1);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

