package MyExamples.pracities2_6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
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

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver","jdbc:mysql://hadoop.spark.com:3306/bigdata","root", "centos");
        Job job = Job.getInstance(conf,"test mysql connection");
        job.setJarByClass(Driver.class);

        job.setMapperClass(ConnMysqlMapper.class);
        job.setReducerClass(ConnMysqlReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("E:\\学习资料\\代码测试数据\\test3.txt"));

        DBOutputFormat.setOutput(job, "mr2mysql", "id1","id2");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
