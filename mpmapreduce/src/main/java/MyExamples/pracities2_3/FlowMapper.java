package MyExamples.pracities2_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-24 22:33
 **/
public class FlowMapper extends Mapper<LongWritable,Text,bean, Text>{
    Text mapKey = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        String[] lineSplit = line.split("\t");
        String phone = lineSplit[1];
        long upFlow = Long.parseLong(lineSplit[lineSplit.length-3]);
        long downFlow = Long.parseLong(lineSplit[lineSplit.length-2]);
        mapKey.set(phone);
        context.write(new bean(upFlow,downFlow),mapKey);
    }
}