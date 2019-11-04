package MyExamples.pracities2_5_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-31 14:36
 **/
public class Driver {
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();

        //本地调试
        //DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver","jdbc:mysql://hadoop.spark.com:3306/bigdata","root", "centos");

        //集群调试
        //DistributedCache.addFileToClassPath(new Path(
         //       "hdfs://hadoop.spark.com:8020/user/medal/jar/mysql-connector-java-5.1.27-bin.jar"), conf);

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://hadoop.spark.com:3306/bigdata", "root", "centos");

        Job job = Job.getInstance(conf,"test mysql connection");
        //集群调试
        job.addFileToClassPath(new Path("hdfs://hadoop.spark.com:8020/user/medal/jar/mysql-connector-java-5.1.27-bin.jar"));


        job.setJarByClass(Driver.class);

        job.setMapperClass(ConnMysqlMapper.class);
        job.setReducerClass(ConnMysqlReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(bean.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/user/medal/mapreduceTest/phone_data.txt"));

        DBOutputFormat.setOutput(job, "static_flow", "phoneNumber","sum_upFlow","sum_downFlow","sum_sum");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
