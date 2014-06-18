package com.secondarysort;

/***************************************
*Driver: SecondarySortBasicDriver
***************************************/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecSortDriver extends Configured implements Tool {

  @Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Two parameters are required for SecondarySortBasicDriver- <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJobName("Secondary sort example");

		job.setJarByClass(SecSortDriver.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SecSortMapper.class);//this is map class
		job.setMapOutputKeyClass(CompositeKeyWritable.class);//this is map output key class.. here we have custom class CompositeKeyWritable
		job.setMapOutputValueClass(NullWritable.class);//This is map output value call.. here it is null
		job.setPartitionerClass(SecSortPartitioner.class); //This gets preference over default hash based partitioner.
//		job.setSortComparatorClass(SecSortCompositeKeySortComparator.class);//This call overwrites compareTo method defined in CompositeKeyWritable class.
		job.setGroupingComparatorClass(SecSortGroupingComparator.class);//This class helps for secondary sort because it compares compositeKeyWritable based on natural key.
		job.setReducerClass(SecSortReducer.class);//This is reducer class
		job.setOutputKeyClass(CompositeKeyWritable.class);//this is output key
		job.setOutputValueClass(NullWritable.class);//this is output value

//		job.setNumReduceTasks(8);
		job.setNumReduceTasks(3);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new SecSortDriver(), args);
		System.exit(exitCode);
	}
}