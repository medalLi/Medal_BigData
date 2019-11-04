package MyExamples.pracities2_2;

import examples.自定义对象.FlowDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author medal
 * @create 2019-03-24 22:34
 **/
public class Driver {
    public static void main(String[] args) throws Exception {
        // 1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 获取jar的存储路径
        job.setJarByClass(FlowDriver.class);

        // 3 关联map和reduce的class类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4 设置map阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(bean.class);

        // 5 设置最后输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(bean.class);

        // 8设置分区
        job.setPartitionerClass(MyPartition.class);
        // 设置reducenum
        job.setNumReduceTasks(5);

        // 6 设置输入数据的路径 和输出数据的路径
        FileInputFormat.setInputPaths(job,new Path("E:/学习资料/代码测试数据/phone_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:/学习资料/代码测试数据/output2"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
