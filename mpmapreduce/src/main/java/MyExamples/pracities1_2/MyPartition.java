package MyExamples.pracities1_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author medal
 * @create 2019-03-24 21:07
 **/
public class MyPartition  extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        int partition =2;
        String line = text.toString();
        char first_char = line.charAt(0);
        if(first_char %2 ==0){
            partition =0;
        }else {
            partition =1;
        }
        return partition;
    }
}
