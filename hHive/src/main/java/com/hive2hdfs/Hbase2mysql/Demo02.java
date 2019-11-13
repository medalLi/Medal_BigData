package com.hive2hdfs.Hbase2mysql;


import com.ly.bean.vn_data_footprint.Foot;
import com.ly.bean.vn_data_footprint.Point;
import com.ly.utils.ConnRestApi;
import com.ly.utils.ConnectionPool;
import com.ly.utils.ListSortUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 足迹接口
 * Created by ly-zhaiqian on 2018/6/4.
 */
public class Demo02 {
   /* static Properties props = new PropertiesUtil("hbase.properties").getProperties();
    static final String JDBC_DRIVER = props.getProperty("jdbc.driver");
    static final String DB_URL = props.getProperty("db.url");*/
    private static final String DB_PHOENIX_URL = "jdbc:phoenix:iov-hadoop-01,iov-hadoop-02,iov-hadoop-03:2181";
    private static final String ZK_QUORUM = "iov-hadoop-01,iov-hadoop-02,iov-hadoop-03";
    private static final String MYSQL_USER = "jdbc.username";
    private static final String MYSQL_PASSWORD = "jdbc.password";
    private static final String INIT_POOL_SIZE = "init.pool.size";
    private static final String MIN_POOL_SIZE = "min.pool.size";
    private static final String MAX_POOL_SIZE = "max.pool.size";
    private static final String SAVE_TABLE_NAME = "V_DATA";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    static final Logger logger = LoggerFactory.getLogger(Demo02.class);

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        String startTime = args[0];
        String endTime = args[1];
        List<Foot> footList = new ArrayList<>();
        List<Point> pointList = new ArrayList<>();

        Demo01 v2 = new Demo01(Long.valueOf(startTime),Long.valueOf(endTime));
        v2.run(footList,pointList);

        SparkConf sparkConf = new SparkConf().setAppName("ExecutePointV2"+args[0]);
        sparkConf.registerKryoClasses(new Class[]{
                ImmutableBytesWritable.class,Result.class,Foot.class,Point.class,List.class});

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("INFO");
        Broadcast<String> bd_tableName = jsc.broadcast(SAVE_TABLE_NAME);
        Broadcast<String> hbase_zk_quorum = jsc.broadcast(ZK_QUORUM);

        JavaRDD<Foot> footJavaRDD = jsc.parallelize(footList, 60);
        JavaRDD<Point> pointJavaRDD = jsc.parallelize(pointList, 60);

        JavaPairRDD<String, Iterable<Foot>> footGroupByKeyRDD = footJavaRDD.mapToPair(new PairFunction<Foot, String, Foot>() {
            @Override
            public Tuple2<String, Foot> call(Foot foot) throws Exception {
                String dcmNo = foot.getDCM_NO();
                return new Tuple2<String, Foot>(dcmNo, foot);
            }
        }).groupByKey();

        JavaPairRDD<String, Iterable<Point>> pointGroupByKeyRDD = pointJavaRDD.mapToPair(new PairFunction<Point, String, Point>() {
            @Override
            public Tuple2<String, Point> call(Point point) throws Exception {
                String dcmNo = point.getDcm_no();
                return new Tuple2<String, Point>(dcmNo, point);
            }
        }).groupByKey();


//        logger.info("==============footGroupByKeyRDD=============== " + footGroupByKeyRDD.collect().size());
//        logger.info("==============pointGroupByKeyRDD============== " + pointGroupByKeyRDD.collect().size());
        //缓存数据
        footGroupByKeyRDD = footGroupByKeyRDD.persist(StorageLevel.DISK_ONLY());
        pointGroupByKeyRDD = pointGroupByKeyRDD.persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<String, Tuple2<Iterable<Foot>, Iterable<Point>>> joinRDD = footGroupByKeyRDD.join(pointGroupByKeyRDD);

        JavaRDD<List<Foot>> mapRDD = joinRDD.map(new Function<Tuple2<String, Tuple2<Iterable<Foot>, Iterable<Point>>>, List<Foot>>() {
            @Override
            public List<Foot> call(Tuple2<String, Tuple2<Iterable<Foot>, Iterable<Point>>> tuple) throws Exception {
                Iterator<Foot> footIterator = tuple._2._1.iterator();
                Iterator<Point> pointIterator = tuple._2._2.iterator();
                List<Foot> foot_list = new ArrayList<Foot>();
                List<Point> point_list = new ArrayList<Point>();
                while (footIterator.hasNext()) {
                    Foot foot = footIterator.next();
                    foot_list.add(foot);
                }
                while (pointIterator.hasNext()) {
                    Point point = pointIterator.next();
                    point_list.add(point);
                }
                return Demo02.calculateFAE(foot_list, point_list);
            }
        });
        System.out.println("======================resultRDD==================" + mapRDD.take(10));
//        QueryRunner query = new QueryRunner(ConnectionPool.getInstance().getDataSource());
        String sql = "insert into iov.t_iov_myfootprint" +
                "(userId,province,city,areaname,lng,lat,dcm_no,trackid,collection_date,address) values(?,?,?,?,?,?,?,?,?,?)";
        try{
            List<List<Foot>> result = mapRDD.collect();
            List<Put> puts = new ArrayList();
            Connection conn = ConnectionPool.getInstance().getDataSource().getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            for(List<Foot> foots : result) {
                for (int i = 0; i < foots.size(); i++) {
//                    System.out.println("===foot====" + foots.get(i));
                    if (foots.get(i) != null && foots.get(i).getProvince() != null && foots.get(i).getCity() != null
                            && foots.get(i).getDistrict() != null) {

//                        Object[] params = {1, foots.get(i).getProvince(), foots.get(i).getCity(), foots.get(i).getDistrict(), foots.get(i).getLon(),
//                                foots.get(i).getLat(), foots.get(i).getDCM_NO(), foots.get(i).getTrackid(), foots.get(i).getCOLLECT_DATE(),
//                                foots.get(i).getAddress()};
//                        query.update(sql, params);

                        ps.setString(1,"1");
                        ps.setString(2,foots.get(i).getProvince());
                        ps.setString(3,foots.get(i).getCity());
                        ps.setString(4,foots.get(i).getDistrict());
                        ps.setString(5,foots.get(i).getLon());
                        ps.setString(6,foots.get(i).getLat());
                        ps.setString(7,foots.get(i).getDCM_NO());
                        ps.setString(8,foots.get(i).getTrackid());
                        ps.setString(9,sdf.format(new Date(Long.valueOf(foots.get(i).getCOLLECT_DATE()+"000"))));
                        ps.setString(10,foots.get(i).getAddress());
                        ps.addBatch();
//                        String rowkey = foots.get(i).getCOLLECT_DATE() + "" + foots.get(i).getDCM_NO();
//                        System.out.println("======rowkey======== " + rowkey);
//                        Put put = new Put(Bytes.toBytes(rowkey));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("USERID"), Bytes.toBytes(1));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("PROVINCE"), Bytes.toBytes(checkIsEmpty(foots.get(i).getProvince())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("CITY"), Bytes.toBytes(checkIsEmpty(foots.get(i).getCity())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("DISTRICT"), Bytes.toBytes(checkIsEmpty(foots.get(i).getDistrict())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("LON"), Bytes.toBytes(checkIsEmpty(foots.get(i).getLon())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("LAT"), Bytes.toBytes(checkIsEmpty(foots.get(i).getLat())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("DCMNO"), Bytes.toBytes(checkIsEmpty(foots.get(i).getDCM_NO())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("TRACKID"), Bytes.toBytes(checkIsEmpty(foots.get(i).getTrackid())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("COLLECT_DATE"), Bytes.toBytes(checkIsEmpty(foots.get(i).getCOLLECT_DATE())));
//                        put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("ADDRESS"), Bytes.toBytes(checkIsEmpty(foots.get(i).getAddress())));
//                        puts.add(put);
                    }
                }
            }
            ps.executeBatch();
            ps.close();
            conn.close();
//            System.out.println("==========puts==============" + puts.size());
//            HbaseUtil.put("V_DATA", puts);
            System.out.println("==========finish==============");
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
//        System.out.println("-----------tableName--------------" + bd_tableName.value());
//        System.out.println("-----------zk_quorum--------------" + hbase_zk_quorum.value());
//        System.out.println("0000000000000000000000000000000000000000000000000000000000000000000000000000");

//        mapRDD.foreachPartition(new VoidFunction<Iterator<List<Foot>>>() {
//            @Override
//            public void call(Iterator<List<Foot>> iterator) throws Exception {
//                org.apache.hadoop.hbase.client.Connection conn = null;
//                Table table = null;
//                try {
//                    System.out.println("-----------tableName--------------" + bd_tableName.value());
//                    System.out.println("-----------zk_quorum--------------" + hbase_zk_quorum.value());
//                    String tableName = bd_tableName.value();
//                    Configuration config = HBaseConfiguration.create();
//                    config.set("hbase.zookeeper.quorum", hbase_zk_quorum.value());
//                    config.set("hbase.zookeeper.property.clientPort", "2181");
//                    table = ConnectionFactory.createConnection(config).getTable(TableName.valueOf(tableName));
//                    System.out.println("===========tableName===========" + Bytes.toString(table.getTableDescriptor().getTableName().getName()));
//                    table.close();
//                    List<Put> puts = new ArrayList();
//                    while(iterator.hasNext()){
//                        List<Foot> foots = iterator.next();
//                            for (int i = 0; i < foots.size(); i++)
//                                if (foots.get(i) != null && foots.get(i).getProvince() != null && foots.get(i).getCity() != null){
////                                        && foots.get(i).getDistrict() != null) {
//                                    String rowkey = System.currentTimeMillis() + "" + foots.get(i).getDCM_NO();
////                                    System.out.println("======rowkey======= " + rowkey);
//                                    Put put = new Put(Bytes.toBytes(rowkey));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("USERID"), Bytes.toBytes(1));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("PROVINCE"), Bytes.toBytes(checkIsEmpty(foots.get(i).getProvince())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("CITY"), Bytes.toBytes(checkIsEmpty(foots.get(i).getCity())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("DISTRICT"), Bytes.toBytes(checkIsEmpty(foots.get(i).getDistrict())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("LON"), Bytes.toBytes(checkIsEmpty(foots.get(i).getLon())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("LAT"), Bytes.toBytes(checkIsEmpty(foots.get(i).getLat())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("DCMNO"), Bytes.toBytes(checkIsEmpty(foots.get(i).getDCM_NO())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("TRACKID"), Bytes.toBytes(checkIsEmpty(foots.get(i).getTrackid())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("COLLECT_DATE"), Bytes.toBytes(checkIsEmpty(foots.get(i).getCOLLECT_DATE())));
//                                    put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("ADDRESS"), Bytes.toBytes(checkIsEmpty(foots.get(i).getAddress())));
//                                    puts.add(put);
//                                }
//                    }
//                      System.out.println("===========hbase puts===========" + puts.size());
//                      HbaseUtil.put("V_DATA", puts);
////                    table.put(puts);
////                    table.close();
//                    logger.info("======数据存入成功！！！" );
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    logger.error("数据插入数据库异常");
//                    System.out.println("========!!!!!!!!!!!!!!!!!!==============" + e.getMessage());
//                }
//            }
//        });

        long end = System.currentTimeMillis();
        System.out.println("=======spend time=======" + (end - start)/1000);
        jsc.close();
    }

    private static long getTimeMillis(String time) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
            SimpleDateFormat dayFormat = new SimpleDateFormat("yy-MM-dd");
            Date curDate = dateFormat.parse(dayFormat.format(new Date()) + " " + time);
            return curDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static String checkIsEmpty(String str){
        if(str == null){
            return "";
        }
        return str;
    }

    /**
     * 判断起始，终止，停止点
     */
    public static List<Foot> calculateFAE(List<Foot> footList, List<Point> pointList) throws ParseException {
        List<Foot> result_foot = new ArrayList<Foot>();
        //list 对象按时间排序
        ListSortUtil.listSort(footList);
        //b 选出 起始点，终止点，停止点
        Foot foot_first = new Foot();
        Foot foot_end = new Foot();
        Foot foot_stop = new Foot();
        Foot foot_stop_start = null;
        Foot foot_stop_end = null;
        for (int i = 0; i < footList.size(); i++) {
            if ("1".equals(footList.get(i).getIgn())) {//起始点
                if (i > 0 && i < footList.size() - 1 && "0".equals(footList.get(i - 1).getIgn())) {
                    foot_first = footList.get(i);
                    foot_first.setFoot_flag(1);
                    if (foot_first.getTrackid() == null) {
                        foot_first.setTrackid(footList.get(i + 1).getTrackid());
                    }
                    result_foot.add(processFoot(foot_first, pointList));
                }
                if (i > 0 && i < footList.size() - 1 && "0".equals(footList.get(i).getVEHICLE_SPEED_CLUSTER())) {
                    //停留点处理
                    if (!"0".equals(footList.get(i - 1).getVEHICLE_SPEED_CLUSTER())
                            && "0".equals(footList.get(i - 1).getHAND_BRAKE_SWITCH())) {
                        //停留起点
                        foot_stop_start = new Foot();
                        foot_stop_start.setCOLLECT_DATE(footList.get(i).getCOLLECT_DATE());
                        foot_stop_start.setHAND_BRAKE_SWITCH(footList.get(i).getHAND_BRAKE_SWITCH());
                        foot_stop_start.setATCVT_RANGE_INDICATION(footList.get(i).getATCVT_RANGE_INDICATION());
                        foot_stop_start.setDCM_NO(footList.get(i).getDCM_NO());
                        foot_stop_start.setENGINE_STATUS(footList.get(i).getENGINE_STATUS());
                        foot_stop_start.setTrackid(footList.get(i).getTrackid());

                    } else if (!"0".equals(footList.get(i + 1).getVEHICLE_SPEED_CLUSTER()) && "0".equals(footList.get(i + 1).getHAND_BRAKE_SWITCH())) {
                        //停留终点
                        foot_stop_end = new Foot();
                        foot_stop_end.setCOLLECT_DATE(footList.get(i).getCOLLECT_DATE());
                        foot_stop_end.setHAND_BRAKE_SWITCH(footList.get(i).getHAND_BRAKE_SWITCH());
                        foot_stop_end.setATCVT_RANGE_INDICATION(footList.get(i).getATCVT_RANGE_INDICATION());
                        foot_stop_end.setDCM_NO(footList.get(i).getDCM_NO());
                        foot_stop_end.setENGINE_STATUS(footList.get(i).getENGINE_STATUS());
                    }
                    if (foot_stop_start != null && foot_stop_end != null) {
                        long stop_times = Long.parseLong(foot_stop_end.getCOLLECT_DATE()) - Long.parseLong(foot_stop_start.getCOLLECT_DATE());
                        if (stop_times >= 10 * 60000) {//停留时间超过10分钟的停留点
                            foot_stop = foot_stop_start;
                            foot_stop.setFoot_flag(0);
                            foot_stop.setStop_time(stop_times / 1000);
                            result_foot.add(processFoot(foot_stop, pointList));
                            foot_stop_start = null;
                            foot_stop_end = null;
                        }
                    }
                }
            } else if ("0".equals(footList.get(i).getIgn())) {
                if (i > 0 && "1".equals(footList.get(i - 1).getIgn())) {
                    foot_end = footList.get(i);
                    foot_end.setFoot_flag(2);
                    if (foot_end.getTrackid() == null) {
                        foot_end.setTrackid(footList.get(i - 1).getTrackid());//终点的行程标记以上一个点为准
                    }
                    result_foot.add(processFoot(foot_end, pointList));
                }
            }
        }
        return result_foot;
    }

    /**
     * 经纬度处理
     */
    public static String calculatelonlat(String param) {
        Float r = Float.parseFloat(param) / 10000000;
        DecimalFormat df = new DecimalFormat("0.0000000000");//格式化小数
        double result = ((Float.parseFloat(param) - Math.floor(r) * 10000000) / 6000000) + Math.floor(r);
        return df.format(result);
    }

    /**
     * 足迹计算
     * a:行程
     * b:经纬度
     * 最后一步 确定点的位置
     */
    public static Foot processFoot(Foot foot, List<Point> pointList) {
        Point point = null;
        try {
            point = Demo02.calculateColltime(pointList, Long.parseLong(foot.getCOLLECT_DATE()));
            //利用高德接口处理坐标
            TimeUnit.MILLISECONDS.sleep(10);
            point = ConnRestApi.getGpsData(point);
            foot.setCountry(point.getCountry());
            foot.setProvince(point.getProvince());
            foot.setCity(point.getCity());
            foot.setDistrict(point.getDistrict());
            foot.setLat(point.getLat());
            foot.setLon(point.getLon());
            foot.setAddress(point.getAddress());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("高德地图接口调用异常" + e.getMessage());
        }
        return foot;
    }

    /**
     * 筛选出最接近的时间
     *
     * @param time
     */
    public static Point calculateColltime(List<Point> list, long time) {
        Point point = new Point();
        if (list.size() > 0) {
            long min_flag = Math.abs(Long.parseLong(list.get(0).getColl_time()) - time);
            point = list.get(0);
            for (int i = 0; i < list.size(); i++) {
                if (Math.abs(Long.parseLong(list.get(i).getColl_time()) - time) < min_flag) {
                    min_flag = Math.abs(Long.parseLong(list.get(i).getColl_time()) - time);
                    point = list.get(i);
                }
            }
        }
        return point;
    }

    /**
     * 执行批量操作
     *
     * @param foots
     */
    public static void executeBatch(List<Foot> foots) {
        Object[][] param = new Object[foots.size()][];
        String sql = "insert into iov.t_iov_myfootprint" +
                "(userId,province,city,areaname,lng,lat,dcm_no,trackid,collection_date,address) values(?,?,?,?,?,?,?,?,?,?)";
        try {
            for (int i = 0; i < foots.size(); i++) {
                if (foots.get(i) != null && foots.get(i).getProvince() != null && foots.get(i).getCity() != null
                        && foots.get(i).getDistrict() != null) {
                    param[i] = new Object[10];
                    param[i][0] = 1;
                    param[i][1] = foots.get(i).getProvince();
                    param[i][2] = foots.get(i).getCity();
                    param[i][3] = foots.get(i).getDistrict();
                    param[i][4] = foots.get(i).getLon();
                    param[i][5] = foots.get(i).getLat();
                    param[i][6] = foots.get(i).getDCM_NO();
                    param[i][7] = foots.get(i).getTrackid();
                    param[i][8] = foots.get(i).getCOLLECT_DATE();
                    param[i][9] = foots.get(i).getAddress();
                }
            }
//            query.batch(sql, param);
            System.out.println("数据存入" + foots.size());

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("数据插入数据库异常");
        }
    }

}
