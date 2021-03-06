package mapreduce.design.pattern.ch02.minmax;
import mapreduce.design.pattern.ch02.MinMaxCountTuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MinMaxCountDriver {
public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args)
			.getRemainingArgs();
	if (otherArgs.length != 2) {
		System.err.println("Usage: MinMaxCountDriver <in> <out>");
		System.exit(2);
	}
	Job job = new Job(conf, "StackOverflow Comment Date Min Max Count");
	job.setJarByClass(MinMaxCountDriver.class);
	job.setMapperClass(MinMaxCountMapper.class);
	job.setCombinerClass(MinMaxCountReducer.class);
	job.setReducerClass(MinMaxCountReducer.class);
	job.setNumReduceTasks(1);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(MinMaxCountTuple.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}