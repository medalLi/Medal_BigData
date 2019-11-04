package examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * KEYIN:输入数据的key  文件的行号
 * VALUEIN：每行的输入数据
 *
 * KEYOUT：输出数据 的key
 * VALUEOUT:输出数据的value类型
 * @author Administrator
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	//hello world
	//atguigu atguigu
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1获取这一行数据
		String line = value.toString();

		// 2 获取每一个单词
		String[] words = line.split("\t");

		for(String word:words){
			// 3 输出每一个单词
			context.write(new Text(word), new IntWritable(1));
		}
	}
}



