package examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	//atguigu 1 atguigu 1
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {

		// 1 统计所有单词个数
		int count = 0;
		for(IntWritable value:values){
			count += value.get();
		}

		// 2输出所有单词个数
		context.write(key, new IntWritable(count));
	}
}
