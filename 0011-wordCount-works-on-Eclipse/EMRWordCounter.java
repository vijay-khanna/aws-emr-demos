package MapReduce.Example0013MapReduceBasics;
/*
 * Make a Maven Project
 * 
 * EMR Map Reducer for Java. 
 * AWS EMR 5.30
 * https://www.youtube.com/watch?v=G6kQ14AAzXQ
 * 
 * to test on local machine. Edit Run configuration, and specify input file and output folder
 * 
 * Remove App.java if it exists
 * 
 * Remove "src/test/java" folder. Right Click Remove from Build Path...
 * 
 * ....When Exporting the jar, Export project as "Runnable Jar", Launch Configuration, Export Destination, Library Handling = "Extract required libraries into generated Jar"
 * 
 * in Run Configuration, Check the run Configurations => Main => Main Class. choose the main driver class. 
 * 
 * 
 * Update Maven project after making the changes . you will see JavaSE 1.8 in JRE System Library. 
 * 
 * POM.XML 
 * <build>
	<plugins>
		<plugin>
			<groupId>org.apache.maven.plugins</groupId>
			<artifactId>maven-compiler-plugin</artifactId>
			<version>3.7.0</version>
			<configuration>
				<source>1.8</source>
				<target>1.8</target>
			
			</configuration>
		</plugin>
	</plugins>
</build>




    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>2.7.3</version>
      
      <scope>test</scope>
    </dependency>
    
   <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.7.3</version>
</dependency>


<!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-mapreduce-client-core</artifactId>
    <version>2.7.3</version>
</dependency>


 <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-mapreduce-client-jobclient</artifactId>
        <version>2.7.3</version>
        <scope>provided</scope>
    </dependency>
    
    </dependencies>
    
    
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EMRWordCounter {
// Main class could have additional below options 
	// Tools is interface that allows to specify command line options using -D . like setting number of reducers 
	// Not mandatory to use Tools and configured , but makes the code more cleaner 
	// extends Configured implements Tool 
	// configured class has two main methods
		// getConf() returns the configuration used by this object. 
		// setConf()
	
	
	
	// Object = Input Key, Text = input Value, Text = Output of Map Process, IntWritable = Output Value = 1 Always <in this case> 1 . for each word
	public static class TokenizeMaper extends Mapper<Object, Text, Text, IntWritable>{
		// we are getting a line of text at a time as input
		
		public void map(Object input_key_for_mapper, Text input_value_for_mapper, Context context_spit_out_keyValue_pairs) throws IOException, InterruptedException {
			//context is the way in which the key-value pairs is spit out. 
				
			IntWritable value_equals_one = new IntWritable(1);
			
		StringTokenizer string_Tokenizer = new StringTokenizer(input_value_for_mapper.toString());
	
			
			/*// Either This section or the next.. both works.. 

			String string_Tokenizer[] = input_value_for_mapper.toString().split("\\s+");
			
			for(String string_Tokenizer_eachWord : string_Tokenizer) {
				context_spit_out_keyValue_pairs.write(new Text(string_Tokenizer_eachWord.replaceAll("[^a-zA-Z]","").toLowerCase()), value_equals_one);
				}
			
			*/
			
		Text key_wordOut = new Text();	
			while(string_Tokenizer.hasMoreTokens()){
//				key_wordOut.set(string_Tokenizer.nextToken());
//				key_wordOut.set(string_Tokenizer.nextToken().toLowerCase());
				key_wordOut.set(string_Tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z]",""));
				// removes all non alphabets, and any special characters 
				
				context_spit_out_keyValue_pairs.write(key_wordOut,value_equals_one);
				
								
			}

			
		}
		
	}
	
	// Input of Reducer needs to be same as Output of Mapper. 
	public static class SumReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
		// System automatically shuffle-sorts, and gives a iterable list as value to this reducer function.
		// e.g. ("hello",1,1,1,1)			hello is key, and 1,1,1,1 is iterable list
		
		public void reduce(Text term, Iterable<IntWritable> list_of_ones, Context context_Write_Aggregate_Output) throws IOException, InterruptedException{
			
			int count = 0;
			Iterator <IntWritable> iterator = list_of_ones.iterator();
			while(iterator.hasNext()) {
				count++;
				iterator.next();
				
			}
			IntWritable output = new IntWritable(count);
			
			context_Write_Aggregate_Output.write(term,output);
						
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// Controller. 
		
		Configuration conf = new Configuration(); // hadoop configuration. 
		
		 String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs(); // 
		 
		 System.out.println("* * *   arguments....");
		 System.out.println(otherArgs[0]);
		 System.out.println(otherArgs[1]);
		 
		 if(otherArgs.length != 2) // check input and output arguments. 
		 {
			 System.err.println("* * * Needs two arguments....usage : WordCount <input_file> <output_folder>");
			 System.exit(2);
		 }
		
		 Job hadoop_job = Job.getInstance(conf, "Word Count");
		 // new job object. Configuration contains information about resources. 
		 // core-site.xml, core-default.xml store default resources.
		 // read only and site specific config of hadoop 
		 
		 
		 hadoop_job.setJarByClass(EMRWordCounter.class);//Identify main driver class. Jar should be on HDFS so that all nodes can find this file.
		
		 hadoop_job.setMapperClass(TokenizeMaper.class);;
		
		 hadoop_job.setReducerClass(SumReducer.class);
		 hadoop_job.setNumReduceTasks(2); // Can increase Reducers with this parameter. 
		 hadoop_job.setOutputKeyClass(Text.class);
		 // we can override MapOutputValueKeyClass if required. 
		 
		 hadoop_job.setOutputValueClass(IntWritable.class);
	//	System.setProperty("hadoop.home.dir","D:\\Softwares\\hadoop\\bin" );
		
		
		FileInputFormat.addInputPath(hadoop_job,new Path(otherArgs[0])); // This can be specified via command line also 
		FileOutputFormat.setOutputPath(hadoop_job,new Path(otherArgs[1]));
		
		boolean isSuccess_JobStatus = hadoop_job.waitForCompletion(true); // submits the job 
		
		if(isSuccess_JobStatus) {
			System.exit(0); // Exit with Success code	
			System.out.println("Completed the Map Reduce Successfully");
			
			
		}
		else
		{
			System.exit(1); // Exit with Failure code # 1
			System.out.println("Exited with Errors");
		}
		
		System.out.println("Completed the Map Reduce");
	}

}
