package SampleProjectsUdemyMR.Example00100SampleProjectsUdemyMR;



/*Facebook Ads Analytics
 * 
 * 
Input File 
AD ID 					:		FKLY490998LB,
Time Stamp				:		2010-01-29 06:12:17,
LocationwhereDisplayed	:		Mumbai,
AdCategory				:		Ecommerce,
NumberofClicks			:		39,
NumberofConversions		:		13,
AgeGroup				:		25-35

Sample Rows
FKLY490998LB,2010-01-29 06:12:17,Mumbai,Ecommerce,39,13,25-35
NIXK800977XM,2010-01-03 00:01:20,Mumbai,Ecommerce,281,5,18-25
CSUR861665UU,2010-01-02 01:41:16,Hyderabad,Health,234,12,45-50
YYZI277313ZG,2010-01-03 17:21:23,Hyderabad,Health,465,9,18-25
XCUI929329UU,2010-01-30 09:22:51,Delhi,Ecommerce,341,9,35-40
TILE144495LE,2010-01-03 05:14:11,Hyderabad,Health,46,15,25-35
ZAGZ980689GX,2010-01-06 07:09:29,Delhi,Education,422,15,50-55
ACVA171663VI,2010-01-07 17:44:31,Bangalore,Ecommerce,338,10,40-45
QXPG660397PK,2010-01-03 19:31:59,Hyderabad,Health,286,7,45-50
PBKO451965KR,2010-01-07 11:05:13,Delhi,Ecommerce,33,14,18-25
IRKP460320KX,2010-01-08 04:21:36,Hyderabad,Fitness,492,10,25-35
KCIE572920IN,2010-01-30 06:55:44,Delhi,Health,175,7,35-40



 * 
 * 
 * Sample Output
Ecommerce	Delhi,9.959075602228252
Ecommerce	Mumbai,21.70423092131277
Ecommerce	Hyderabad,3.7405241539525353
Ecommerce	Bangalore,3.264700694344221
Education	Delhi,3.7862362817507034
Education	Hyderabad,2.0242914979757085
Fitness	Delhi,5.947396558341087
Fitness	Mumbai,1.834862385321101
Fitness	Hyderabad,4.934546750311793
Fitness	Bangalore,12.782362257230977
Health	Delhi,3.055658587111336
Health	Mumbai,2.742759599081902
Health	Hyderabad,6.338267305260219
Health	Bangalore,1.8306636155606408
 * 
 * 
 * Question : Need to find out the Average of Each Category Location wise
 * e.g. average success Rate of ECommerce Rate for Delhi, Mumbai etc.. there are multiple lines for Mumbai and Delhi ettc.. 
 */





import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FacebookDriver 
{
	public static void main(String[] args) throws IOException,	ClassNotFoundException, InterruptedException 
	{

	//	Path input_path = new Path("hdfs://localhost:9000/user/jivesh/fb");
	//	Path output_dir = new Path("hdfs://localhost:9000/user/jivesh/output");

        

		
		
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Facebook analysis");

		// name of driver class
		job.setJarByClass(FacebookDriver.class);
		// name of mapper class
		job.setMapperClass(FacebookMapper.class);
		// name of reducer class
		job.setReducerClass(FacebookReducer.class);
					
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

	//	FileInputFormat.addInputPath(job, input_path);
	//	FileOutputFormat.setOutputPath(job, output_dir);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		
		
		
		
	//	output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,	true);

		job.waitForCompletion(true);
	}
}
