package testinputformat;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Concatenates all the values for given keys recieved from mapper

public class MyOldReducer extends MapReduceBase
    implements Reducer<Text, LongWritable, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterator<LongWritable> values,
                     OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String allval = "";
        while (values.hasNext()) {
            LongWritable value = values.next();
            allval += (Long.toString(value.get())  + ",");
        } 
        output.collect(key,new Text(allval.substring(0,allval.length() - 1)));
    }
}
