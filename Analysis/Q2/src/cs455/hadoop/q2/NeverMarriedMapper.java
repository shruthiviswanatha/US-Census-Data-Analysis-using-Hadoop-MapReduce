package cs455.hadoop.q2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NeverMarriedMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	MapWritable mw = new MapWritable();
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100){
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 1) {
				String state = line.substring(8, 10);
				// get total no. of never married males
				String nmMale = line.substring(4422, 4431);
				int neverMarriedMale = Integer.parseInt(nmMale);
				// get total no. of never married female
				String nmFemale = line.substring(4467, 4476);
				int neverMarriedFemale = Integer.parseInt(nmFemale);
				// total block population
				int blockPop = Integer.parseInt(line.substring(300, 309));
				
				mw.put(new IntWritable(0), new IntWritable(neverMarriedMale));   // nm male
				mw.put(new IntWritable(1), new IntWritable(neverMarriedFemale)); // nm female
				mw.put(new IntWritable(2), new IntWritable(blockPop));           // block population
				
				context.write(new Text(state), mw);
				}
			}
	}
}
