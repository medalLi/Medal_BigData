package cn.itcast01;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.ZooKeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.fusesource.leveldbjni.All;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
* HBaseAdmin(Admin) : 管理表（创建，删除）
*       HTableDescriptor:表描述器，用于创建表
*       HColunDescriptor:列描述器（构建列族）
*
* Table：用于表中数据的操作
*       Put:用于封装待存放的数据
*       Delete:用于封装待删除的数据
*       Get:用于得到某一个具体的数据
*
* Scan：用于扫描表的配置信息
* ResultScanner:通过配置的扫描器，得到一个扫描表的实例扫描器
* Result:每一个类型的实例化对象，都对应了一个rowKey中的若干数据
* Cell:用于封装一个rowKey下面所有单元格中的数据(rowkey,cf,cn,value)
* */
public class HbaseDemo {
    public static Configuration conf;
    static{
        //使用HBase Configuration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop.spark.com");
        conf.set("hbase.zookeeper.property.client Port", "2181");
    }

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        //在HBase 中管理、访问表需要先创建HBaseAdmin对象
        //老API
       // HBaseAdmin admin = new HBaseAdmin(conf);
        //新API
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin =  connection.getAdmin();

        return admin.tableExists(TableName.valueOf(tableName));
    }

    //创建表
    public static void createTable(String tableName, String... columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin =  connection.getAdmin();
        //判断表是否存在
        if(isTableExist(tableName)){
            System.out.println(" 表 " + tableName + " 已存在 ");
            //System.exit(0);
        }else{
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for(String cf : columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println(" 表" + tableName + " 创建成功！ ");
        }
    }

    //删除表
    public static void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin =  connection.getAdmin();
        if(isTableExist(tableName)){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(" 表 " + tableName + " 删除成功！ ");
        }else{
            System.out.println(" 表 " + tableName + " 不存在！ ");
        }
    }

    //向表中插入数据
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException{
        //创建HTable对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        //HTable hTable = new HTable(conf, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        //向Put对象中组装数据
        table.put(put);
        table.close();
        System.out.println(" 插入数据成功 ");
    }

    //删除多行数据
    public static void deleteRow(String tableName, String rowKey,String cf) throws IOException{
        //创建HTable对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
       /* for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }*/
        table.delete(delete);
        table.close();
    }

    //删除多行数据
    public static void deleteMultiRow(String tableName, String... rows) throws IOException{
        //创建HTable对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
       table.delete(deleteList);
        table.close();
    }

    //得到所有数据
    public static void getAllRows(String tableName) throws IOException{
        //创建HTable对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println(" 行键:" + Bytes.toString(CellUtil.cloneRow(cell)));

                //得到列族
                System.out.println(" 列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
    //得到某一行所有数据
    public static void getRow(String tableName, String rowKey) throws IOException{
        //创建HTable对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.set Max Versions();显示所有版本

        //get.set Time Stamp();显示指定时间戳的版本

        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println(" 行键:" + Bytes.toString(result.getRow()));
            System.out.println(" 列族:" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println(" 时间戳:" + cell.getTimestamp());
        }
    }

    //获取某一行指定“列族：列”
    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException{
        //创建HTable对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println(" 行键:" + Bytes.toString(result.getRow()));
            System.out.println(" 列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void main(String[] args) throws Exception {
        boolean result =isTableExist("ns_ct:calllog");
        System.out.print("result :"+result);
    }




    }
