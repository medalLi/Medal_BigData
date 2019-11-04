package MyExamples.pracities1_6;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author medal
 * @create 2019-03-24 17:27
 **/
public class WordReducer extends TableReducer<Text,LongWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        int sum = 0;
        for(LongWritable iw : values){
            sum = sum +1;
        }
        //初始化put对象
        Put put = new Put(key.getBytes());//put实例化，每一个词存一行
        //列族为content,列修饰符为count，列值为数目
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
        context.write(new ImmutableBytesWritable(key.getBytes()), put);// 输出求和后的<key,value>

    }
}
