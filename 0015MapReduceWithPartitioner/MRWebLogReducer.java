package MapReduce.Example0015MRPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class MRWebLogReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text key_HostIPAddress, Iterable<Text> iterableList_values_Months, Context context)
			throws IOException, InterruptedException {


		int count_HostIPAddress_Hits = 0;
		// the iterable list will look like
		//("10.10.10.1",(Apr, Apr, Apr))

		for (@SuppressWarnings("unused")
		Text each_Month : iterableList_values_Months) {
			count_HostIPAddress_Hits++;
		}

		context.write(key_HostIPAddress, new IntWritable(count_HostIPAddress_Hits));
		// Each Reducer for each month will print IP and Count of IP hits 
	}
}
