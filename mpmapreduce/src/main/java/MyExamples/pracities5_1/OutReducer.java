package MyExamples.pracities5_1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-28 10:38
 **/
public class OutReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        context.write(key,NullWritable.get());
    }
}
