package MyExamples.pracities1_7;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author medal
 * @create 2019-03-30 15:07
 **/
public class HbaseOutputFormat extends OutputFormat<Text, LongWritable> {
    private OutputCommitter committer = null;
    @Override
    public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new HbaseRecordWriter(context);
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

    static class HbaseRecordWriter extends RecordWriter<Text,LongWritable> {
        private TaskAttemptContext context = null;
        HBaseDao hd = null;

        public HbaseRecordWriter(TaskAttemptContext context) {
            this.context = context;
        }

        @Override
        public void write(Text key, LongWritable value) throws IOException, InterruptedException {
             hd = new HBaseDao();
            String oriValue = key.toString();
            hd.put(oriValue+"\t"+value.get());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }

}

