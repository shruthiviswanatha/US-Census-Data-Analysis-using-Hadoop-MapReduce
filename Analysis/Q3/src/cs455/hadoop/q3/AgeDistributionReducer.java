package cs455.hadoop.q3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AgeDistributionReducer extends Reducer<Text, MapWritable, Text, Text> {
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int under18male = 0;
		int under18female = 0;
		int btw19to29male = 0;
		int btw19to29female = 0;
		int btw30to39male = 0;
		int btw30to39female = 0;
		int tHispPopulation = 0;
		
		for (MapWritable value : values ) {
			int a = ((IntWritable) value.get(new IntWritable(0))).get();
			under18male += a;
			int b = ((IntWritable) value.get(new IntWritable(1))).get();
			under18female += b;
			int c = ((IntWritable) value.get(new IntWritable(2))).get();
			btw19to29male += c;
			int d = ((IntWritable) value.get(new IntWritable(3))).get();
			btw19to29female += d;
			int e = ((IntWritable) value.get(new IntWritable(4))).get();
			btw30to39male += e;
			int f = ((IntWritable) value.get(new IntWritable(5))).get();
			btw30to39female += f;
			int p = ((IntWritable) value.get(new IntWritable(6))).get();
			if (p < 0) {
				System.out.println("[ERROR] At reducer\t" + key.toString() + " : " + p);
			}
			tHispPopulation += p;
		}
		// calculate percentage of hispanic population based on gender and age distribution
		Float percentunder18Male = (under18male/ (float) tHispPopulation) * 100;
		Float percentunder18Female = (under18female/ (float) tHispPopulation) * 100;
		Float percentbtw19to29Male = (btw19to29male/ (float) tHispPopulation) * 100;
		Float percentbtw19to29Female = (btw19to29female/ (float) tHispPopulation) * 100;
		Float percentbtw30to39Male = (btw30to39male/ (float) tHispPopulation) * 100;
		Float percentbtw30to39Female = (btw30to39female/ (float) tHispPopulation) * 100;
		System.out.println("T : " + tHispPopulation + "\tU18M : " + under18male);
		context.write(key,new Text(new String("Under 18 Hispanic Male: " + percentunder18Male.toString() + ". Under 18 Hispanic Female: " + percentunder18Female.toString() + ". Hispanic Male between 19 to 29: " + percentbtw19to29Male.toString() + ". Hispanic Female between 19 to 29: " + percentbtw19to29Female.toString() + ". Hispanic Male between 30 to 39: " +percentbtw30to39Male.toString() + ". Hispanic Female between 30 to 39: " +percentbtw30to39Female.toString())));
	}
}
