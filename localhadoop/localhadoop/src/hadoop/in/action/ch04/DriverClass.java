package hadoop.in.action.ch04;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import wordcount.WordCountMapper;
import wordcount.WordCountReducer;
 
public class DriverClass {
 
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
//        conf.set("fs.default.name", "hdfs://localhost:9000");
//        conf.set("mapred.job.tracker", "localhost:9001");
        
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Create job
        Job job = new Job(conf, "DriverClass");
        job.setJarByClass(MapperClass.class);
        
        
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
//        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
        
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Setup MapReduce
//        MAP
       job.setMapperClass(MapperClass.class);
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(IntWritable.class);
        
        
//        REDUCE
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(1);
 
        // we can also specify global key and value class only if both MAP and reduce useses same kind of key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
    // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
 
}
