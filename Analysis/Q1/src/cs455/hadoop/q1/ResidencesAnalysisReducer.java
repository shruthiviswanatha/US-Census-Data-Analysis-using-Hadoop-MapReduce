package cs455.hadoop.q1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ResidencesAnalysisReducer extends Reducer<Text, MapWritable, Text, Text> {

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int owned = 0;
		int rented = 0;
		for (MapWritable value : values ) {
			int a = ((IntWritable) value.get(new IntWritable(0))).get();
			owned += a;
			int b = ((IntWritable) value.get(new IntWritable(1))).get();
			rented += b;
		}
		//calculate percentage houses owned and rented
		float total = owned + rented;
		Float percentOwned = ( owned/ total) * 100;
		Float percentRented = ( rented/ total) * 100;

		context.write(key,new Text(new String("% owned: " + percentOwned.toString() + ". % rented: " + percentRented.toString())));

	}
}
