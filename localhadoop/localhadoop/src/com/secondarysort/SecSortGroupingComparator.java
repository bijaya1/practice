package com.secondarysort;

/***************************************************************
*GroupingComparator: SecondarySortBasicGroupingComparator
***************************************************************/
 
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
 
public class SecSortGroupingComparator extends WritableComparator {
  protected SecSortGroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}
 
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key1.getDeptNo().compareTo(key2.getDeptNo());
	}
}