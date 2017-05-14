package cs455.hadoop.q4;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class HouseholdDistributionReducer extends Reducer<Text, MapWritable, Text, Text> {
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int urban  = 0;
		int rural = 0;
		float total = 0;
		int nUrbRur = 0;
		
		for (MapWritable value : values ) {
			int u = ((IntWritable) value.get(new IntWritable(0))).get();
			urban += u;
			int r = ((IntWritable) value.get(new IntWritable(1))).get();
			rural += r;
			float t = ((IntWritable) value.get(new IntWritable(2))).get();
			total += t;
			int n = ((IntWritable) value.get(new IntWritable(3))).get();
			nUrbRur += n;
		}
		// calculate percentage of total urban and rural households
		Float percentUrban = ((float)urban/ total) * 100;
		Float percentRural = ((float)rural/ total) * 100;
		context.write(key,new Text(new String("% of Urban Households: " + percentUrban.toString() + ". % of Rural Households: " + percentRural.toString())));
	}
}
