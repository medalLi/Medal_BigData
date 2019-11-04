package cn.maprduce;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		Put put = new Put(key.get());
		//遍历该rowkey下面的所有单元格
		for(Cell cell : value.rawCells()){
			//如果当前单元格访问到的数据是info列族，则进行下一步操作
			if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
				//提取name列
				if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					//如果数据需要清洗和转换则需要取出具体数据，然后重新封装Cell
					put.add(cell);
				}else if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					put.add(cell);
				}
			}
		}
		
		context.write(key, put);
	}
	
}
