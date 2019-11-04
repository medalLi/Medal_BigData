package MyExamples.pracities5_1;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author medal
 * @create 2019-03-28 10:40
 **/
public class Output extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new OutRecordWriter(taskAttemptContext);
    }

    static class OutRecordWriter extends RecordWriter<Text,NullWritable>{
        FSDataOutputStream fos1 = null;
        FSDataOutputStream fos2 = null;

        public OutRecordWriter(TaskAttemptContext job){
            FileSystem fs;
            try {
                fs = FileSystem.get(job.getConfiguration());
                Path path1 = new Path("/user/medal/mapreduceTest/log1.txt");
                Path path2 = new Path("/user/medal/mapreduceTest/log2.txt");

                fos1 = fs.create(path1);
                fos2 = fs.create(path2);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            String line = text.toString();
            if(line.contains("135")){
                fos1.write(line.getBytes());
                fos1.writeUTF("\n");


            }else{
                fos2.write(line.getBytes());
                fos2.writeUTF("\n");
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(fos1 != null){
                fos1.close();
            }

            if(fos2 != null){
                fos2.close();
            }
        }
    }
}
