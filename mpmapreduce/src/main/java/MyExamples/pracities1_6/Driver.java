package MyExamples.pracities1_6;


import MyExamples.pracities1_5.MySqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author medal
 * @create 2019-03-24 17:27
 **/
public class Driver {
    public static void main(String[] args) throws Exception{
        String tablename = "ns_ct:wordcount";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop.spark.com");
//
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        //使用WordCountHbaseMapper类完成Map过程；
        job.setMapperClass(WordMapper.class);
        TableMapReduceUtil.initTableReducerJob(tablename, WordReducer.class, job);

        //设置任务数据的输入路径；
        FileInputFormat.addInputPath(job, new Path("/user/medal/mapreduceTest/phone_data.txt"));
        //设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；
        job.setOutputKeyClass(Text.class);
        //设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为IntWritable；
        job.setOutputValueClass(LongWritable.class);
        //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }


}
