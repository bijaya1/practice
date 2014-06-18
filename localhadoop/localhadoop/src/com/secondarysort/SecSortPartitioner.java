package com.secondarysort;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
 
public class SecSortPartitioner extends Partitioner<CompositeKeyWritable, NullWritable> {
 
	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value, int numReduceTasks) {
 
		return (key.getDeptNo().hashCode() % numReduceTasks);
	}
}