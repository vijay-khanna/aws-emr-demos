package MapReduce.Example0015MRPartitioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class  MRWebLogMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static List<String> months = Arrays.asList("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");

  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   *
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String[] fields = value.toString().split(" ");
    
    if (fields.length > 3) {
      String incoming_host_ip = fields[0]; // 0 Index field is IP : E.g:  96.7.4.14
      String[] dateTime = fields[3].split("/"); // Take 3rd index field, and then split the DateTime.. Eg : 24/Apr/2011:04:20:11
      if (dateTime.length > 1) {
        String theMonth = dateTime[1]; // Extract the month from DateTime Field.. Eg :  Apr
     
        if (months.contains(theMonth))
        	context.write(new Text(incoming_host_ip), new Text(theMonth));
        // Key is the incoming host IP
        // Value is Month. 
        
      }
    }
  }
}
