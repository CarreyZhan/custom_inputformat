package mapreduce.hi.api.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomInputFormat extends FileInputFormat<LongWritable, Text>{

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        
        String delimiter = context.getConfiguration().get(
            "inputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
          recordDelimiterBytes = delimiter.getBytes();
        assert recordDelimiterBytes != null : "should set inputformat.record.delimiter";
        return new CustomRecordReader(recordDelimiterBytes);
    }

    @Override
    public boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
