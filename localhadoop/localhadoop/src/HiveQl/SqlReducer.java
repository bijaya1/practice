package HiveQl;


import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class SqlReducer extends
        Reducer<Text, Text, Text, FloatWritable> {
	int ErrorCount=0;
    int TotalCount=0;
 
    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
//    	ID          TotalCount   ErrorCount   DT
//    	----------------------------------------------
//    	1345653         5           3       20120709
//    	534140349       5           2       20120709
//    	SELECT 100* sum(ErrorCount*1.0)/ sum(TotalCount) FROM MyTablewhere dt ='20120709';
//    	key is ErrorCount and value is TotalCount
//    	(ErrorCount+","+TotalCount);
    	
    	float sum = 0;
        for (Text value : values) {
//            sum += Integer.parseInt(value.toString());
//        	System.out.println(value.toString());
        	String[] csv = value.toString().split(",");
        	ErrorCount += Integer.parseInt(csv[0]);
        	TotalCount += Integer.parseInt(csv[1]);
        }
//        System.out.println(ErrorCount);
//        System.out.println(TotalCount);
        sum = (float) (100*(ErrorCount*1.0)/TotalCount);
        context.write(text, new FloatWritable(sum));
    }
}