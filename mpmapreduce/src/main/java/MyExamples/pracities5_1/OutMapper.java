package MyExamples.pracities5_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-28 10:37
 **/
public class OutMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text mapKey = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        context.write(value,NullWritable.get());

    }
}
