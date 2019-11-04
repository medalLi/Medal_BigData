package examples.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable>{

	// hadoop 
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// 需求：按照单词首字母的ASCII的奇偶分区
		String line = key.toString();

		// 1 截取首字母
		String firword = line.substring(0, 1);

		// 2 转换成ASCII
		char[] charArray = firword.toCharArray();

		int result = charArray[0];

		// 3 按照奇偶分区
		if (result % 2 ==0 ) {
			return 0;
		}else {
			return 1;
		}
	}
}
