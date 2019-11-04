package MyExamples.pracities1_7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-24 17:27
 **/
public class WordReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        int sum = 0;
        for(LongWritable iw : values){
            sum = sum +1;
        }
       context.write(key,new LongWritable(sum));
    }
}
