package MyExamples.pracities2_6;

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
        String tbl_name;
        String tbl_type;
        public TblsWritable()
        {

        }
        public TblsWritable(String tbl_name,String tab_type)
        {
            this.tbl_name = tbl_name;
            this.tbl_type = tab_type;
        }
        @Override
        public void write(PreparedStatement statement) throws SQLException
        {
            statement.setString(1, this.tbl_name);
            statement.setString(2, this.tbl_type);
        }
        @Override
        public void readFields(ResultSet resultSet) throws SQLException
        {
            this.tbl_name = resultSet.getString(1);
            this.tbl_type = resultSet.getString(2);
        }
        @Override
        public void write(DataOutput out) throws IOException
        {
            out.writeUTF(this.tbl_name);
            out.writeUTF(this.tbl_type);
        }
        @Override
        public void readFields(DataInput in) throws IOException
        {
            this.tbl_name = in.readUTF();
            this.tbl_type = in.readUTF();
        }
        public String toString()
        {
            return new String(this.tbl_name + " " + this.tbl_type);
        }
    }
}
