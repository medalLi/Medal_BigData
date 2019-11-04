package MyExamples.pracities1_6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author medal
 * @create 2019-03-24 17:26
 **/
public class WordMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
    Text mapkey = new Text();
    LongWritable mapvalue = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line);
        while(st.hasMoreTokens()){
            String line1 = st.nextToken();
            mapkey.set(line1);
            context.write(mapkey,mapvalue);
        }
    }
}
