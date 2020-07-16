package MapReduce.Example0015MRPartitioner;

/*
 * Using Partitioner ensure the keys are send to specific reducers.  
 * 
 * Case here is we have a web log with below schema. 
 * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   * Source IP,-,-, Time , HTTP Request, success code, Number of Bytes
   * 
   * Q : Need to count the number of distinct requests per host, and then aggregate these per month.
   * How many times a specific IP/Hostname made HTTP request per month.
   * 
   * We need 12 different web logs, and each file should list Each IP with the # HTTP Request per IP. 
   * First File = Jan
   * Last File  = Dec
   * 
 */




import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class  MRWebLogDriverMainClass extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new  MRWebLogDriverMainClass(), args);
        System.exit(returnStatus);
    }


public int run(String[] args) throws IOException{



	@SuppressWarnings("deprecation")
	Job hadoop_MR_job = new Job(getConf());
	
	
	hadoop_MR_job.setJobName("MapReduce Apache Logs using Log Partitioner");
	
	hadoop_MR_job.setJarByClass( MRWebLogDriverMainClass.class);

	hadoop_MR_job.setOutputKeyClass(Text.class);
	hadoop_MR_job.setOutputValueClass(IntWritable.class);
	hadoop_MR_job.setMapOutputKeyClass(Text.class);
	hadoop_MR_job.setMapOutputValueClass(Text.class);
	 
	hadoop_MR_job.setMapperClass( MRWebLogMapper.class);
	hadoop_MR_job.setReducerClass( MRWebLogReducer.class);
	hadoop_MR_job.setPartitionerClass( MRWebLogPartitioner.class);
	 // Default implementation takes 1 Reducer class.. 
	hadoop_MR_job.setNumReduceTasks(12); // we can hard code it here, or send the parameter via command line -D parameter. 
	 
	
	FileInputFormat.addInputPath(hadoop_MR_job, new Path(args[0]));
	FileOutputFormat.setOutputPath(hadoop_MR_job,new Path(args[1]));
	   	
	
	try {
		return hadoop_MR_job.waitForCompletion(true) ? 0 : 1;
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return 0;
	
	
  
}


}
