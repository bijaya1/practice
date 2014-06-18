package com.secondarysort;

/***************************************************************
*CustomWritable for the composite key: CompositeKeyWritable
****************************************************************/

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 * 
 * 
 *         Purpose: A custom writable with two attributes- deptNo and
 *         NameEmpIDPair; 
 */

public class CompositeKeyWritable implements Writable,
  	WritableComparable<CompositeKeyWritable> {

	private String deptNo;
	private String flnameempid;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String deptNo, String lNameEmpIDPair) {
		this.deptNo = deptNo;
		this.flnameempid = lNameEmpIDPair;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(deptNo).append("\t")
				.append(flnameempid)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		deptNo = WritableUtils.readString(dataInput);
		flnameempid = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, deptNo);
		WritableUtils.writeString(dataOutput, flnameempid);
	}

	public int compareTo(CompositeKeyWritable objKeyPair) {
		// TODO:
		/*
		 * Note: This code will work as it stands; but when CompositeKeyWritable
		 * is used as key in a map-reduce program, it is de-serialized into an
		 * object for comapareTo() method to be invoked;
		 * 
		 * To do: To optimize for speed, implement a raw comparator - will
		 * support comparison of serialized representations
		 */
		int result = deptNo.compareTo(objKeyPair.deptNo);
		if (0 == result) {
			result = -flnameempid.compareTo(objKeyPair.flnameempid);
		}
		return result;
	}

	public String getDeptNo() {
		return deptNo;
	}

	public void setDeptNo(String deptNo) {
		this.deptNo = deptNo;
	}

	public String getLNameEmpIDPair() {
		return flnameempid;
	}

	public void setLNameEmpIDPair(String lNameEmpIDPair) {
		this.flnameempid = lNameEmpIDPair;
	}
}