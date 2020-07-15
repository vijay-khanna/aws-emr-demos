package mapreducedemo.mapreducedemo;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;



// EMR Map Reducer for Java. 
// EMR 5.30
// https://www.youtube.com/watch?v=G6kQ14AAzXQ


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCounter {

	public static class TokenizeMaper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString());
			Text wordOut = new Text();
			IntWritable one = new IntWritable(1);
			
			while(st.hasMoreTokens()){
				wordOut.set(st.nextToken());
				context.write(wordOut,one);
				
			}
			
			
		}
		
	}
	
	public static class SumReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
		
		public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException{
			
			int count = 0;
			Iterator <IntWritable> iterator = ones.iterator();
			while(iterator.hasNext()) {
				count++;
				iterator.next();
				
			}
			IntWritable output = new IntWritable(count);
			
			context.write(term,output);
			
			
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		 
		 if(otherArgs.length != 2)
		 {
			 System.err.println("* * * Needs two arguments....usage : WordCount <input_file> <output_folder>");
			 System.exit(2);
		 }
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(WordCounter.class);
		
		job.setMapperClass(TokenizeMaper.class);;
		
		job.setReducerClass(SumReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.setProperty("hadoop.home.dir","D:\\Softwares\\hadoop\\bin" );
		
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		boolean status = job.waitForCompletion(true);
		
		if(status) {
			System.exit(0); // Exit with Success code	
			System.out.println("Completed the Map Reduce Successfully");
		}
		else
		{
			System.exit(1); // Exit with Failure code # 1
		}
		
		
	}

}
