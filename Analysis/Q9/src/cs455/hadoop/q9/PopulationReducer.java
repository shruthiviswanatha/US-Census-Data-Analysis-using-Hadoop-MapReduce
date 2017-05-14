package cs455.hadoop.q9;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Iterator;

public class PopulationReducer extends Reducer<Text, IntWritable, Text, Text> {
	private final static int NUM_STATES = 50;
	private LinkedList<String> unknownSts = new LinkedList<String>();
	private static final Map<String, String> myMap = createMap();
	private Map<String, Float> stPop = new HashMap<String, Float>();
	float totalPopulation = 0;
	private boolean headerNotDone = true;
	private static Map<String, String> createMap(){
		Map<String, String> myMap = new HashMap<String, String>();
		myMap.put("AL","alabama");
		myMap.put("AK","alaska");
		myMap.put("AZ","arizona");
		myMap.put("AR","arkansas");
		myMap.put("CA","california");
		myMap.put("CO","colorado");
		myMap.put("CT","connecticut");
		myMap.put("DE","delaware");
		myMap.put("FL","florida");
		myMap.put("GA","georgia");
		myMap.put("HI","hawaii");
		myMap.put("ID","idaho");
		myMap.put("IL","illinois");
		myMap.put("IN","indiana");
		myMap.put("IA","iowa");
		myMap.put("KS","kansas");
		myMap.put("KY","kentucky");
		myMap.put("LA","louisiana");
		myMap.put("ME","maine");
		myMap.put("MT","montana");
		myMap.put("NE","nebraska");
		myMap.put("NV","nevada");
		myMap.put("NH","new hampshire");
		myMap.put("NJ","new jersey");
		myMap.put("NM","new mexico");
		myMap.put("NY","new york");
		myMap.put("NC","north carolina");
		myMap.put("ND","north dakota");
		myMap.put("OH","ohio");
		myMap.put("OK","oklahoma");
		myMap.put("OR","oregon");
		myMap.put("MD","maryland");
		myMap.put("MA","massachusetts");
		myMap.put("MI","michigan");
		myMap.put("MN","minnesota");
		myMap.put("MS","mississippi");
		myMap.put("MO","missouri");
		myMap.put("PA","pennsylvania");
		myMap.put("RI","rhode island");
		myMap.put("SC","south carolina");
		myMap.put("SD","south dakota");
		myMap.put("TN","tennessee");
		myMap.put("TX","texas");
		myMap.put("UT","utah");
		myMap.put("VT","vermont");
		myMap.put("VA","virginia");
		myMap.put("WA","washington");
		myMap.put("WV","west virginia");
		myMap.put("WI","wisconsin");
		myMap.put("WY","wyoming");
		return myMap;
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		float total = 0;
		float percentTotal = 0;
		
		for (IntWritable value : values ) {
			int p = value.get();
			total += p;    // total population of each state
		}
		
		totalPopulation += total;
		
		if (headerNotDone) {
			context.write(new Text(""),new Text(",stateName,Percentage_Population"));
			headerNotDone = false;
		}
		
		if (!myMap.containsKey(key.toString())) {
			// ignore for territories other than 50 states
			unknownSts.add(key.toString());
		} else if (!stPop.containsKey(key.toString())) {
			stPop.put(key.toString(), new Float(total));
		}
		
		if (stPop.size() >= NUM_STATES && unknownSts.size() >= 3) {
			// the total count is 53 out of which we just want the percentages of 50 states
			Iterator it = stPop.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry kV = (Map.Entry)it.next();
				total = ((Float) kV.getValue()).floatValue();
				percentTotal = (total / (float) totalPopulation) * 100;
				context.write(new Text(myMap.get(kV.getKey()) + "," + myMap.get(kV.getKey())),
								new Text("," + new Float(percentTotal).toString()));
			}
		}
		
		System.out.println(key + "\t" + totalPopulation + "\t" + total);
	}
}
