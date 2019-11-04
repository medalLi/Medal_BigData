package MyExamples.pracities2_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-24 22:33
 **/
public class FlowReducer extends Reducer<bean,Text,bean,Text>{
    @Override
    protected void reduce(bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        for(Text te : values){
            context.write(key,te);
        }
    }
}
