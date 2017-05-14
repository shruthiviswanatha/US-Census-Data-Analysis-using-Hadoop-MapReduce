package cs455.hadoop.q1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ResidencesAnalysisMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	MapWritable mw = new MapWritable();
	int pCnt = 0;
	final int LIMIT = 25;
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100) {
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 2) {
				String state = line.substring(8, 10);
				// calculate number of owned residences
				String numOwn = line.substring(1803, 1812);
				int numOwned = Integer.parseInt(numOwn);
				// calculate number of rented residences
				String numRent = line.substring(1812, 1821);
				int numRented = Integer.parseInt(numRent);
				
				mw.put(new IntWritable(0), new IntWritable(numOwned));
				mw.put(new IntWritable(1), new IntWritable(numRented));
				
				context.write(new Text(state), mw);
			}
		}
	}
}
