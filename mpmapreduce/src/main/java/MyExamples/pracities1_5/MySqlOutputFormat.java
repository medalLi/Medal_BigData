package MyExamples.pracities1_5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class MySqlOutputFormat extends OutputFormat<Text, LongWritable> {
    private OutputCommitter committer = null;
    @Override
    public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        //初始化JDBC连接器对象
        Connection conn = null;
        conn = JDBCInstance.getInstance();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }

        return new MysqlRecordWriter(conn);
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

    static class MysqlRecordWriter extends RecordWriter<Text,LongWritable> {
        private Connection conn = null;
        private PreparedStatement preparedStatement = null;
        private String insertSQL = null;

        private int count = 0;
        private final int BATCH_SIZE = 2;
        public MysqlRecordWriter(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void write(Text key, LongWritable value) throws IOException, InterruptedException {
            try {

                if (insertSQL == null) {
                    insertSQL = "INSERT INTO `wordCount` (`word`, `count`) values (?,?) ;";
                }

                if (preparedStatement == null) {
                    preparedStatement = conn.prepareStatement(insertSQL);
                }

                preparedStatement.setString(1, key.toString());
                preparedStatement.setLong(2, value.get());
                preparedStatement.addBatch();
                count++;
                if(count >= BATCH_SIZE){
                    preparedStatement.executeBatch();
                    conn.commit();
                    count = 0;
                    preparedStatement.clearBatch();
                }
                System.out.println("preparedStatement+++++++++++++++++++++++++++++++");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                if(preparedStatement != null){
                    preparedStatement.execute();
                    this.conn.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                JDBCUtil.close(conn, preparedStatement, null);
            }
        }
    }

}
