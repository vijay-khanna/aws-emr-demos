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

		String line = value.toString();
		/* Split csv string */
		String[] words = line.split(",");        // [ {FKLY490998LB} {2009-12-29 06:12:17} {Mumbai} {Ecommerce} {39} {13} {25-35} ]

		
		c.write(new Text(words[3]), new Text (words[2] + "," + words[4] + "," +  words[5]));
		
	    }     // Ecommerce                   Mumbai        ,   39          ,      13  
	}
