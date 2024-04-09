package testinputformat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapreduce.hi.api.input.CustomInputFormat;

public class CustomRecordReaderDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.printf("Usage: %s [generic options] <input> <output> <Mapper-Y/N> <Reduced-Y/N> <queue_name>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job = Job.getInstance(getConf(), "Test Custom Record Reader");

//  As out mapper flips key and value recieved from record reader, we need to reset 
//  Output Key and Value classes based on our decision to use mapper or not
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);

        if (args[2].compareTo("Y") == 0) {
            job.setMapperClass(MyMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
        }

        // We either use our own mapper or no mapper at all.
        if (args[3].compareTo("Y") == 0) {
            job.setReducerClass(MyReducer.class);
        } else {

            job.setNumReduceTasks(0);
        }

        String queueName = args[4];
        job.getConfiguration().set("mapreduce.job.queuename", queueName);
        
        job.setInputFormatClass(CustomInputFormat.class);
        
//  This code tests our code across split boundaries. 16 bytes will split the files in 3        
        FileInputFormat.setMaxInputSplitSize(job, 16);
        FileInputFormat.setMinInputSplitSize(job, 1);
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem filesystem = FileSystem.get(getConf());
        filesystem.delete(new Path(args[1]), true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CustomRecordReaderDriver(), args);
        System.exit(exitCode);
    }
}
