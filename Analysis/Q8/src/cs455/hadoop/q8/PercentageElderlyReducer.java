package cs455.hadoop.q8;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.LinkedList;

public class PercentageElderlyReducer extends Reducer<Text, MapWritable, Text, Text> {
	private static final int NUM_STATES = 53;
	LinkedList<Float> percentage = new LinkedList<Float>();
	LinkedList<String> states = new LinkedList<String>();
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int ageAbove85 = 0;
		int totalPopulation = 0;
		int j=0;
		Float highestPercentage = (float) 0.0;
		String stateOld = "NotAState";
		float percent = 0;
		
		for (MapWritable value : values ) {
			int age85 = ((IntWritable) value.get(new IntWritable(0))).get();
			ageAbove85 += age85;
			int popn = ((IntWritable) value.get(new IntWritable(1))).get();
			totalPopulation += popn; // calculate total population of state
		}
		
		percent = (((float)ageAbove85/ (float)totalPopulation) * 100);
		percentage.add(percent);
		states.add(key.toString());
		
		if(percentage.size() >= NUM_STATES) {
			for(j=0; j<NUM_STATES; j++){
				if (percentage.get(j) > highestPercentage) {
					// determine state with greatest proportion of old people
					highestPercentage = percentage.get(j);
					stateOld = states.get(j);
				}
			}
			context.write(new Text(stateOld), new Text(new String("Highest % of elderly people age > 85: " + highestPercentage.toString())));
		}
		
	}
}
