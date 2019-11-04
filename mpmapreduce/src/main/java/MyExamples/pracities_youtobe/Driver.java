package MyExamples.pracities_youtobe;

/**
 * @author medal
 * @create 2019-03-28 22:32
 **/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author medal
 * @create 2019-03-24 17:27
 **/
public class Driver {
    public static void main(String[] args) throws Exception{
        // 1 获取job对象信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar位置
        job.setJarByClass(Driver.class);

        // 3 设置mapper和reducer的class类
        job.setMapperClass(YoutobeMapper.class);
        //job.setReducerClass(WordReducer.class);

        // 4 设置输出mapper的数据类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终数据输出的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 处理小文件
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path("/user/medal/mapreduceTest/youtube/0.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/medal/mapreduceTest/youtube/output"));

        // 8 添加分区
//        job.setPartitionerClass(WordCountPartitioner.class);
       job.setNumReduceTasks(0);

        // 7 提交
        boolean result = job.waitForCompletion(true);


        System.exit(result ? 0 : 1);

    }


}

