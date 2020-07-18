package SampleProjectsUdemyMR.Example00100SampleProjectsUdemyMR;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//FKLY490998LB,2009-12-29 06:12:17,Mumbai,Ecommerce,39,13,25-35
public class FacebookMapper extends Mapper<LongWritable, Text, Text, Text>
{
	 
	    @Override
	    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
	    {

		String eachline_line_of_FB_Stream = value.toString();
		/* Split csv string */
		String[] words = eachline_line_of_FB_Stream.split(",");        // [ {FKLY490998LB} {2009-12-29 06:12:17} {Mumbai} {Ecommerce} {39} {13} {25-35} ]

		/*
			Input File 
0			AD ID 					:		FKLY490998LB,
1			Time Stamp				:		2010-01-29 06:12:17,
2			LocationwhereDisplayed	:		Mumbai,
3			AdCategory				:		Ecommerce,
4			NumberofClicks			:		39,
5			NumberofConversions		:		13,
6			AgeGroup				:		25-35
		 */
		
		
		
		
		c.write(new Text(words[3]), new Text (words[2] + "," + words[4] + "," +  words[5])); // Mapper emits this key-value pair
			// Key = AdCategory,  Value = LocationwhereDisplayed +	NumberofClicks + NumberofConversions
	    }   // 	Ecommerce                   Mumbai        			,   39          	,       13  
	    	//	AdCategory					LocationwhereDisplayed		NumberofClicks			NumberofConversions
	    	
	    // Could have stored keys value in a String, and then output if required. 
	    
	    
	}
