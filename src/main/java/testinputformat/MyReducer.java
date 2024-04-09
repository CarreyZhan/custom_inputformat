package testinputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Concatenates all the values for given keys recieved from mapper

public class MyReducer 
    extends Reducer<Text, LongWritable, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<LongWritable> values,
                      Context context) throws IOException, InterruptedException {
        String allval = "";
        for (LongWritable value : values) {
            allval += (Long.toString(value.get())  + ",");
        } 
        context.write(key,new Text(allval.substring(0,allval.length() - 1)));
    }
}
