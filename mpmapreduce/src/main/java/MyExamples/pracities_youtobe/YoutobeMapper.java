package MyExamples.pracities_youtobe;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-28 22:32
 **/
public class YoutobeMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = ETLUtil.method(value.toString());
        text.set(line);
        context.write(NullWritable.get(),text);
    }
}
