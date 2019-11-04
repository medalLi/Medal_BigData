package MyExamples.pracities2_5_error;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author medal
 * @create 2019-03-25 20:57
 **/
public class MySqlOutputFormat extends OutputFormat<Text, bean> {
    private OutputCommitter committer = null;
    @Override
    public RecordWriter<Text, bean> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        //初始化JDBC连接器对象


//        try {
//            System.out.println("到这儿了！！！！！");
//            if(conn == null){

//            }
//           // conn.setAutoCommit(false);
//        } catch (SQLException e) {
//            throw new RuntimeException(e.getMessage());
//        }
        return new MysqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        //输出校检
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if(committer == null){
            String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
            Path outputPath = name == null ? null : new Path(name);
            committer = new FileOutputCommitter(outputPath, context);
        }
        return committer;
    }

    static class MysqlRecordWriter extends RecordWriter<Text, bean> {
        private Connection conn = JDBCInstance.getInstance();
        private PreparedStatement preparedStatement = null;
        private String insertSQL = null;

        private int count = 0;
        private final int BATCH_SIZE = 3;
        //public MysqlRecordWriter(Connection conn) {
            //this.conn = conn;
       // }
        @Override
        public void write(Text key, bean value) throws IOException, InterruptedException {
            try {
                if (insertSQL == null) {
                    insertSQL = "INSERT INTO `static_flow` " +
                            "(`phoneNumber`, `sum_upFlow`,`sum_downFlow`,`sum_sum`) " +
                            "values (?,?,?) ;";
                }

               // if (preparedStatement == null) {
                    preparedStatement = conn.prepareStatement(insertSQL);
               // }

                preparedStatement.setString(1, key.toString());
                preparedStatement.setLong(2, value.getUpFlow());
                preparedStatement.setLong(3, value.getDownFlow());
                preparedStatement.setLong(4, value.getSumFlow());
                //preparedStatement.addBatch();
                preparedStatement.execute();
//                count++;
//                if(count >= BATCH_SIZE){
//                    preparedStatement.executeBatch();
//                    conn.commit();
//                    System.out.println("提交成功！！！！！");
//                    count = 0;
//                    preparedStatement.clearBatch();
//                }
                System.out.println("preparedStatement+++++++++++++++++++++++++");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

            JDBCUtil.close(conn, preparedStatement, null);

        }
    }

}
