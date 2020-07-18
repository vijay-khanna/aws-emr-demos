package SampleProjectsUdemyMR.Example00100SampleProjectsUdemyMR;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FacebookReducer  extends Reducer<Text, Text, Text, Text>
{
	//Ecommerce    [ { Mumbai,39,13 }  Mumbai,281,5 } {Delhi,341,9} {Delhi,398,10}................]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
    {

          //     key: city value:total_success_rate, count
	HashMap<String, String> cityData = new HashMap<String, String>();   //       [{Mumbai:65.07,3 } {Delhi:69.65,7} {Bangalore:9.78,3} ...... ]
	
	Iterator<Text> itr = values.iterator();
	/* all data for each category */
	while (itr.hasNext())
	{
	    String f = itr.next().toString();       // f =  Mumbai,281,5

	    String[] words = f.split(",");                 //   words = [ {Mumbai} {281} {5}]
	    
	    String location = words[0].trim();            // location = Mumbai
	    
	    int clickCount = Integer.parseInt(words[1]);   // clickCount= 281
	    
	    int conversionCount = Integer.parseInt(words[2]);   //  conversionCount = 5 
	    
	    Double succRate = new Double(conversionCount/(clickCount*1.0)*100);   // succRate = 1.77

	    if (cityData.containsKey(location))
	    {
	    String s1 = cityData.get(location);  // s1 =  33.3,1
		String[] hValues = s1.split(",");         // hValues = [ {33.3} {1} ]            
		Double totalSuccRate = Double.parseDouble(hValues[0]) + succRate;    // totalSuccRate  = (33.3 + 1.77_ = 35.07 
		int totalCount = Integer.parseInt(hValues[1]) + 1;             // totalCount =  2

		cityData.put(location, totalSuccRate + "," + totalCount);

	    }else
	    {
		cityData.put(location, succRate + ",1");
	    }
	}
            System.out.println(cityData.toString());
	for (Map.Entry<String, String> e : cityData.entrySet())        // e = Mumbai  value  65.07,3
	{
	    String[] V1 = e.getValue().split(",");        // V1  [{65.07} {3}]
	    Double avgSccRate = Double.parseDouble(V1[0])/Integer.parseInt(V1[1]);
	    c.write(key, new Text(e.getKey() + "," + avgSccRate));
	}
    }
}
