package MyExamples.pracities2_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author medal
 * @create 2019-03-25 10:26
 **/
public class MyPartition extends Partitioner<Text,bean> {
    @Override
    public int getPartition(Text text, bean bean, int i) {
        int partions = 0;
        String line = text.toString();
        if(line.substring(0,3).equals("135")){
            partions = 1;
        }else if(line.substring(0,3).equals("136")){
            partions =2;
        }else if(line.substring(0,3).equals("137")){
            partions =3;
        }else{
            partions =4;
        }

        return partions;
    }
}
