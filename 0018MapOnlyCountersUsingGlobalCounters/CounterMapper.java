package MapOnlyCountersusingGlobalCounters;
/*
 // obtain simple statistics. count the number of people who submitted email and mobile numbers during the aadhaar registration.  

*/
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

/**
 *
 
 
 
 
 */
public class CounterMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
	enum InformationCounter{EMAIL,MOBILE};
	// Java Enumerator Class. 	Object with Fields Email and Mobile
	// Global Counter, to store values
	

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   // will get one line at a time as input 
    String[] fields = value.toString().split(","); // split each line
    if (fields.length > 1) {
      int email = Integer.parseInt(fields[10]);
      int mobile = Integer.parseInt(fields[11]);
      if (email==1) {
          context.getCounter(InformationCounter.EMAIL).increment(1); // Increment Global Counter
          // Not writing anything in mapper class. just updating the Global Counters. 
       }
      if (mobile==1) {
    	  context.getCounter(InformationCounter.MOBILE).increment(1);
      }
    }
  }
}