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
	//Key, <Iterable List of Values>
	
	//Reducer needs to do two things. 1. calculate Success Rate of Each Record, and then Average it.. 
	
    @Override
    protected void reduce(Text AdCategory_key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
    {

          //     value: city value:total_success_rate, count
	HashMap<String, String> cityData_HashMap = new HashMap<String, String>();   //       [{Mumbai:65.07,3 } {Delhi:69.65,7} {Bangalore:9.78,3} ...... ]
// collections are dynamic and play vital role for MR programs. 
	
	
	
	Iterator<Text> itr = values.iterator(); // iterates the values of city,adds,impressions . Each Reducer does it for a category
	/* all data for each category */
	while (itr.hasNext()) // iteration loop
	{
	    String Element_of_valuesList = itr.next().toString();       // f =  Mumbai,281,5
	    String[] words_Array_Iterator_List = Element_of_valuesList.split(",");                 //   words = [ {Mumbai} {281} {5}]
	    String location = words_Array_Iterator_List[0].trim();            // location = Mumbai
	    int clickCount = Integer.parseInt(words_Array_Iterator_List[1]);   // clickCount= 281
	    int conversionCount = Integer.parseInt(words_Array_Iterator_List[2]);   //  conversionCount = 5 
	    Double successRate = new Double(conversionCount/(clickCount*1.0)*100);   // succRate = 1.77

	    if (cityData_HashMap.containsKey(location))
	    {
	    	// get each success rate and current_Total_count
	    	
	    String string_ClicksAndConversions = cityData_HashMap.get(location);  // string =  33.3,1
		String[] array_ClicksAndConversions = string_ClicksAndConversions.split(",");         // hValues = [ {33.3} {1} ]            
		Double summationOfSuccessRates = Double.parseDouble(array_ClicksAndConversions[0]) + successRate;    // totalSuccRate  = (33.3 + 1.77_ = 35.07)... Adds all the success Rates. 
		int totalCount = Integer.parseInt(array_ClicksAndConversions[1]) + 1;             // totalCount =  2  // counts all items...

		cityData_HashMap.put(location, summationOfSuccessRates + "," + totalCount);

	    }else
	    {
	    	cityData_HashMap.put(location, successRate + ",1"); // Create a new entry if HashMap does not contain an entry for the City already. 
	    }
	}
            System.out.println("Key : "+AdCategory_key+ "            Value :" +cityData_HashMap.toString());
            
	for (Map.Entry<String, String> cityData_HashMap_EachEntry : cityData_HashMap.entrySet())        // e = Mumbai  value  65.07,3
	{
	    String[] Array_cityData_HashMap_EachEntry = cityData_HashMap_EachEntry.getValue().split(",");        //  [{65.07} {3}]		
	    Double averageSuccessRate = Double.parseDouble(Array_cityData_HashMap_EachEntry[0])/Integer.parseInt(Array_cityData_HashMap_EachEntry[1]);// find average success Rate
	    //Array_cityData_HashMap_EachEntry[0] = Total successRate of all entries for a Category+City
		//Array_cityData_HashMap_EachEntry[1] = Total Entries for a Category+City
	    		
	    c.write(AdCategory_key, new Text(cityData_HashMap_EachEntry.getKey() + "," + averageSuccessRate)); // cityData_HashMap_EachEntry.getKey() gives the City Name from HashMap-Key
	    //	Example... 
	    //	Ecommerce	Bangalore,3.264700694344221
	    // 	Education	Delhi,3.7862362817507034
	
	}
    }
}
