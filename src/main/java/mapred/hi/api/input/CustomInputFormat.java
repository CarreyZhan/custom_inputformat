package mapred.hi.api.input;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class CustomInputFormat extends FileInputFormat<LongWritable, Text>{

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
            JobConf conf, Reporter reporter) throws IOException {
        return new CustomRecordReader(conf, split);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }
}
