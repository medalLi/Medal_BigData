package MyExamples.pracities2_5_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author medal
 * @create 2019-03-31 14:33
 **/
public class ConnMysqlReducer extends Reducer<Text,bean, WriteDataToMysql.TblsWritable, WriteDataToMysql.TblsWritable>
{
    public void reduce(Text key,Iterable<bean> values,Context context)throws IOException,
            InterruptedException
    {
        //接收到的key value对即为要输入数据库的字段，所以在reduce中：
        //wirte的第一个参数，类型是自定义类型TblsWritable，利用key和value将其组合成TblsWritable，
        //然后等待写入数据库
        //wirte的第二个参数，wirte的第一个参数已经涵盖了要输出的类型，所以第二个类型没有用，设为null
        Long sum_upFlow =0L;
        Long sum_downFlow =0L;
        for(bean be : values)
        {
            sum_upFlow = sum_upFlow + be.upFlow;
            sum_downFlow = sum_downFlow + be.downFlow;
        }
        context.write(new WriteDataToMysql.TblsWritable(key.toString(),sum_upFlow.toString(),sum_downFlow.toString()),null);
    }

}

