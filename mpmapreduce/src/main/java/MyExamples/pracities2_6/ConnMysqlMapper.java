package MyExamples.pracities2_6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-31 14:32
 **/
public class ConnMysqlMapper  extends Mapper<LongWritable, Text,Text,Text>
        //TblsRecord是自定义的类型，也就是上面重写的DBWritable类
{
    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
    {
        //<首字母偏移量,该行内容>接收进来，然后处理value，将abc和x作为map的输出
        //key对于本程序没有太大的意义，没有使用
        String name = value.toString().split("\t")[0];
        String type = value.toString().split("\t")[1];
        context.write(new Text(name),new Text(type));
    }
}
