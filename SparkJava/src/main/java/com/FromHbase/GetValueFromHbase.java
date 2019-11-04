package com.FromHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @author medal
 * @create 2019-04-01 22:43
 **/
public class GetValueFromHbase {
    public static void main(String[] args) {
//        JavaSparkContext sc = new JavaSparkContext("spark://hadoop.spark.com:7077",
//                "hbaseTest");
        SparkConf conf1 = new SparkConf().setAppName("hbaseTest");

        JavaSparkContext sc = new JavaSparkContext(conf1);

        Configuration conf = HBaseConfiguration.create();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"));


        try {
            String tableName = "fruit";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class);
            System.out.println(myRDD.count());
            //myRDD.foreach();
            myRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
                public void call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    //Result.
                    for (Cell cell : immutableBytesWritableResultTuple2._2.rawCells()) {
                        System.out.println(//
                                Bytes.toString(CellUtil.cloneFamily(cell)) + ":" //
                                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + " ->" //
                                        + Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
            });
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
