package MyExamples.pracities2_5_error;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-24 22:33
 **/
public class FlowReducer extends Reducer<Text, bean,Text, bean>{
    @Override
    protected void reduce(Text key, Iterable<bean> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        long sum_upFlow =0;
        long sum_downFlow =0;
        for(bean be : values){
            sum_upFlow = sum_upFlow + be.upFlow;
            sum_downFlow = sum_downFlow + be.downFlow;
        }

        context.write(key,new bean(sum_upFlow,sum_downFlow));
    }
}
