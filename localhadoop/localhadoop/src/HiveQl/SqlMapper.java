package HiveQl;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class SqlMapper extends
        Mapper<Object, Text, Text, Text> {
 
    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    private Text compositeValue = new Text();
    String ErrorCount;
    String TotalCount;
 
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
 
//    store line into array
//    	ID          TotalCount   ErrorCount   DT
//    	----------------------------------------------
//    	1345653         5           3       20120709
//    	534140349       5           2       20120709
//    	SELECT 100* sum(ErrorCount*1.0)/ sum(TotalCount) FROM MyTablewhere dt ='20120709';

        String[] csv = value.toString().split("\\s+");
//        for (int i = 0; i < csv.length; i++) {
//			System.out.println(i+","+csv[i]);
//		}
        
        if (csv[3].equals("20120710")){
//        	System.out.println("Find the 20120710");
        	
        	ErrorCount =  csv[2];
        	TotalCount =  csv[1];
//        	TotalCount =  Integer.parseInt(csv[1]);
            compositeValue.set(ErrorCount+","+TotalCount);
        
            word.set(new Text("20120710"));
            context.write(word, compositeValue);
       
    }
    }
}