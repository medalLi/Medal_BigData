package cn.maprduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

public class WriteFruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Put put : value){
			context.write(NullWritable.get(), put);
		}
	}
	

}
