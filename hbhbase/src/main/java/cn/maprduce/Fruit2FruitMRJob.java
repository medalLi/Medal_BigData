package cn.maprduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2FruitMRJob extends Configured implements Tool {
	
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		Fruit2FruitMRJob f2f = new Fruit2FruitMRJob();
		try {
			int status = ToolRunner.run(conf, f2f, args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		//组装Job
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Fruit2FruitMRJob.class);
		
		Scan scan = new Scan();
		//scan.setCacheBlocks(false);
		//scan.setCaching(500);
		
		TableMapReduceUtil.initTableMapperJob(
				"fruit",//Mapper操作的表名
				scan,//扫描表的对象是谁
				ReadFruitMapper.class,//制定Mapper类
				ImmutableBytesWritable.class,//制定Mapper的输出key
				Put.class,//指定Mapper的输出Value
				job//指定Job
				);
		
		TableMapReduceUtil.initTableReducerJob("fruit_mr", 
				WriteFruitReducer.class, 
				job);
		
		job.setNumReduceTasks(1);
		boolean isSucceed = job.waitForCompletion(true);
		
		return isSucceed ? 0 : 1;
	}

}
