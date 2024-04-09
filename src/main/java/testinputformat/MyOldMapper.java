package testinputformat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.StringTokenizer;

//   Our mapper tokenizes the value received from record reader and uses these as
//   keys while the key is returned as value for each of these tokens.
public class MyOldMapper extends MapReduceBase
        implements  Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        StringTokenizer tokens = new StringTokenizer(value.toString(),",");
        while (tokens.hasMoreTokens()) {
            output.collect(new Text(tokens.nextToken()), key);
        }
    }
    
}
