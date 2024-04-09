package testinputformat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

//   Our mapper tokenizes the value received from record reader and uses these as
//   keys while the key is returned as value for each of these tokens.
public class MyMapper 
        extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer tokens = new StringTokenizer(value.toString(),",");
        while (tokens.hasMoreTokens()) {
            context.write(new Text(tokens.nextToken()), key);
        }
    }
    
}
