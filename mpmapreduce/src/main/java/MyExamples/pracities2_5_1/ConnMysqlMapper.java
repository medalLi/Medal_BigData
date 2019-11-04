package MyExamples.pracities2_5_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-31 14:32
 **/
public class ConnMysqlMapper  extends Mapper<LongWritable, Text,Text, bean>
        //TblsRecord是自定义的类型，也就是上面重写的DBWritable类
{
    Text mapKey = new Text();
    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
    {
        //<首字母偏移量,该行内容>接收进来，然后处理value，将abc和x作为map的输出
        //key对于本程序没有太大的意义，没有使用
        String line = value.toString();
        String[] lineSplit = line.split("\t");
        String phone = lineSplit[1];
        long upFlow = Long.parseLong(lineSplit[lineSplit.length-3]);
        long downFlow = Long.parseLong(lineSplit[lineSplit.length-2]);
        mapKey.set(phone);
        context.write(mapKey,new bean(upFlow,downFlow));
    }
}
