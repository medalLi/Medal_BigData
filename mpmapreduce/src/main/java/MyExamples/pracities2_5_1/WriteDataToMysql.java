package MyExamples.pracities2_5_1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author medal
 * @create 2019-03-31 14:30
 **/
public class WriteDataToMysql {
    /**
     * 重写DBWritable
     * @author asheng
     * TblsWritable需要向mysql中写入数据
     */
    public static class TblsWritable implements Writable, DBWritable
    {
        String phone;
        String upFlow;
        String downFlow;
        String sumFlow;

        public TblsWritable()
        {

        }
        public TblsWritable(String phone,String upFlow,String downFlow)
        {
            this.phone = phone;
            this.upFlow = upFlow;
            this.downFlow = downFlow;
            this.sumFlow = Long.parseLong(upFlow)+Long.parseLong(downFlow) +"";
        }
        @Override
        public void write(PreparedStatement statement) throws SQLException
        {
            statement.setString(1, this.phone);
            statement.setString(2, this.upFlow);
            statement.setString(3, this.downFlow);
            statement.setString(4, this.sumFlow);
        }
        @Override
        public void readFields(ResultSet resultSet) throws SQLException
        {
            this.phone = resultSet.getString(1);
            this.upFlow = resultSet.getString(2);
            this.downFlow = resultSet.getString(3);
            this.sumFlow = resultSet.getString(4);
        }
        @Override
        public void write(DataOutput out) throws IOException
        {
            out.writeUTF(this.phone);
            out.writeUTF(this.upFlow);
            out.writeUTF(this.downFlow);
            out.writeUTF(this.sumFlow);
        }
        @Override
        public void readFields(DataInput in) throws IOException
        {
            this.phone = in.readUTF();
            this.upFlow = in.readUTF();
            this.downFlow = in.readUTF();
            this.sumFlow = in.readUTF();
        }
        public String toString()
        {
            return new String(this.phone + " " + this.upFlow+ " " + this.downFlow+ " " + this.sumFlow);
        }
    }
}
