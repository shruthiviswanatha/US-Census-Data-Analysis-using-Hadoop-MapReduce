package cs455.hadoop.q4;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HouseholdDistributionMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		int totalHouses=0;
		int urbanHouse = 0;
		int ruralHouse = 0;
		int notUrbRur = 0;
		MapWritable mw = new MapWritable();
		
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100){
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 2) {
				String state = line.substring(8, 10);
				// calculate total no. of urban and rural households
				String u = line.substring(1821,1830);
				urbanHouse = Integer.parseInt(u);
				u = line.substring(1830,1839);
				urbanHouse += Integer.parseInt(u);
					
				String r = line.substring(1839, 1848);
				ruralHouse = Integer.parseInt(r);
				
				String na = line.substring(1848, 1857);
				notUrbRur = Integer.parseInt(na);
				// calculate total households
				totalHouses = urbanHouse + ruralHouse + notUrbRur;
				mw.put(new IntWritable(0), new IntWritable(urbanHouse));
				mw.put(new IntWritable(1), new IntWritable(ruralHouse));
				mw.put(new IntWritable(2), new IntWritable(totalHouses));
				mw.put(new IntWritable(3), new IntWritable(notUrbRur));
				
				context.write(new Text(state), mw);
			}
		}
	}
}

