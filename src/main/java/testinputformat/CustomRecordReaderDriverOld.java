package testinputformat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapred.hi.api.input.CustomInputFormat;

public class CustomRecordReaderDriverOld extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.printf("Usage: %s [generic options] <input> <output> <Mapper-Y/N> <Reduced-Y/N> <queue_name>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration conf = new Configuration();
        JobConf job = new JobConf(conf);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        if (args[2].compareTo("Y") == 0) {
            job.setMapperClass(MyOldMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
        }

        // We either use our own mapper or no mapper at all.
        if (args[3].compareTo("Y") == 0) {
            job.setReducerClass(MyOldReducer.class);
        } else {
            job.setNumReduceTasks(0);
        }

        String queueName = args[4];
        job.set("mapreduce.job.queuename", queueName);
        
        job.setInputFormat(CustomInputFormat.class);
        
//  This code tests our code across split boundaries. 16 bytes will split the files in 3        
        //FileInputFormat.setMinSplitSize(1);
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem filesystem = FileSystem.get(getConf());
        filesystem.delete(new Path(args[1]), true);

        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CustomRecordReaderDriver(), args);
        System.exit(exitCode);
    }
}
