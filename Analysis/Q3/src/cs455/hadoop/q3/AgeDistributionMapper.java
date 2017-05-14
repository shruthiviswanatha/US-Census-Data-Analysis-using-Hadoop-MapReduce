package cs455.hadoop.q3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeDistributionMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	final int POP_FIELD_LEN = 9;
	final int POP_BEGIN_IDX = 3864;
	final int POP_END_IDX = 4413;
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		int totalHispanic=0;
		int under18Male=0;
		int between19to29Male=0;
		int between30to39Male=0;
		int under18Female=0;
		int between19to29Female=0;
		int between30to39Female=0;
		MapWritable mw = new MapWritable();
		
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100){
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 1) {
				String state = line.substring(8, 10);
				
				for(int startIndex = POP_BEGIN_IDX; startIndex <= POP_END_IDX; startIndex += POP_FIELD_LEN) {
					
					// calculate total hispanic population
					String a = line.substring(startIndex, startIndex + POP_FIELD_LEN);
					int numHisp = Integer.parseInt(a);
					totalHispanic += numHisp;
					// calculate hispanic population based on age distribution and gender
					if(startIndex <= 3972) {
						under18Male += numHisp;
					}
					
					if(startIndex >= 3981 && startIndex <= 4017) {
						between19to29Male += numHisp;
					}
					
					if(startIndex >= 4026 && startIndex <= 4035) {
						between30to39Male += numHisp;
					}
					
					if(startIndex >= 4143 && startIndex <= 4251){
						under18Female +=numHisp;
					}
					
					if(startIndex >= 4260 && startIndex <= 4296){
						between19to29Female +=numHisp;
					}
					
					if(startIndex >= 4305 && startIndex <= 4314){
						between30to39Female += numHisp;
					}
				}
				
				if (totalHispanic < 0) {
					System.out.println("[ERROR]\t" + state + " : " + totalHispanic);
				}
				mw.put(new IntWritable(0), new IntWritable(under18Male));
				mw.put(new IntWritable(1), new IntWritable(under18Female));
				mw.put(new IntWritable(2), new IntWritable(between19to29Male));
				mw.put(new IntWritable(3), new IntWritable(between19to29Female));
				mw.put(new IntWritable(4), new IntWritable(between30to39Male));
				mw.put(new IntWritable(5), new IntWritable(between30to39Female));
				mw.put(new IntWritable(6), new IntWritable(totalHispanic));
				
				context.write(new Text(state), mw);
			}
		}
	}
}
