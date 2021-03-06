package MyExamples.pracities1_4;


import MyExamples.pracities1_3.MyCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        //大量小文件优化
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);

        // 4 设置输出mapper的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        // 5 设置最终数据输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path("E:\\学习资料\\代码测试数据\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\学习资料\\代码测试数据\\output"));


        // 7 提交
        boolean result = job.waitForCompletion(true);


        System.exit(result ? 0 : 1);

    }


}
