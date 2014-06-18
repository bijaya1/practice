package hadoop.in.action.ch04;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 
public class ReducerClass extends
        Reducer<Text, IntWritable, Text, IntWritable> {
 
    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
    	Iterator<IntWritable> iter = values.iterator();
//        String csv = "";
    	int sum = 0;
        
        while(iter.hasNext()){
//        if (csv.length() > 0) csv += ",";
//            csv += iter.next().toString();
        	sum += iter.next().get();
        }
        
        
//        context.write(text, new Text(csv));
        context.write(text, new IntWritable(sum));
    }
}
