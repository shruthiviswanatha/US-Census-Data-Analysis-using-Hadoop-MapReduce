package cs455.hadoop.q2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NeverMarriedReducer extends Reducer<Text, MapWritable, Text, Text> {

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int nmMale = 0;
		int nmFemale = 0;
		int tPopulation = 0;
		for (MapWritable value : values ) {
			int a = ((IntWritable) value.get(new IntWritable(0))).get();
			nmMale += a;
			int b = ((IntWritable) value.get(new IntWritable(1))).get();
			nmFemale += b;
			int p = ((IntWritable) value.get(new IntWritable(2))).get();
			tPopulation += p;
		}
		// calculate percentage of never married male and female
		Float percentNMMale = ( (float) nmMale/ (float) tPopulation) * 100;
		Float percentNMFemale = ( (float) nmFemale/ (float) tPopulation) * 100;
		Float percentNeverMarried = ((nmMale + nmFemale)/(float)tPopulation) * 100;
		context.write(key,new Text(new String("Never Married Male: " + percentNMMale.toString() + ". Never Married Female: " + percentNMFemale.toString() + ". Never Married both Male & Female: " + percentNeverMarried.toString())));
	}
}
