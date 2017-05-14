package cs455.hadoop.q8;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PercentageElderlyMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		MapWritable mw = new MapWritable();
		
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100){
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 1) {
				String state = line.substring(8, 10);
				String age = line.substring(1065,1074);
				int age85above = Integer.parseInt(age);
				
				String pop = line.substring(300,309);
				int population = Integer.parseInt(pop);
					
				mw.put(new IntWritable(0), new IntWritable(age85above));
				mw.put(new IntWritable(1), new IntWritable(population));
				
				context.write(new Text(state), mw);
			}
		}
	}
}

