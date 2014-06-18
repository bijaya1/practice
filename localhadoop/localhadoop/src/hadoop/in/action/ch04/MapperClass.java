package hadoop.in.action.ch04;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
public class MapperClass extends
Mapper<Object, Text, Text, IntWritable> {

//private final IntWritable ONE = new IntWritable(1);
//private Text key1 = new Text();


public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
	IntWritable one = new IntWritable(1);
	Text key1 = new Text();

String[] csv = value.toString().split("\\s+");
//System.out.println("input key = "+csv[0]);
//System.out.println("input value = "+csv[1]);
//key1.set(csv[0]);
//for (int i = 1; i < csv.length; i++) {
	
////}
////for (String str : csv) {
//    
//    context.write(key1, new IntWritable(Integer.parseInt(csv[i])));
    key1.set(csv[1]);
	context.write(key1, one);

//}
}
}