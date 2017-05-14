package cs455.hadoop.q5;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MedianOwnedMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		int rangeValue = 0;
		int startIdx = 2928;
		int endIdx = 2937;
		MapWritable mw = new MapWritable();
		
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100){
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 2) {
				String state = line.substring(8, 10);
				
				for(int i=0; i < 20 ; i++){
					String r = line.substring(startIdx,endIdx);
					int range = Integer.parseInt(r);
					rangeValue = range;
					mw.put(new IntWritable(i), new IntWritable(rangeValue));
					startIdx += 9;
					endIdx += 9;
				}
				context.write(new Text(state), mw);
			}
		}
	}
}

