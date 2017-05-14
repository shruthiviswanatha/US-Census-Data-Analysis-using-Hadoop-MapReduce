package cs455.hadoop.q7;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AvgRoomsMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	
	static final int pLIMIT = 0;
	int              pCnt   = 0;
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		int startIdx = 2388;
		int endIdx = startIdx+9;
		MapWritable mw = new MapWritable();
		
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100){
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 2) {
				String state = line.substring(8, 10);
				
				for(int i=0; i < 9 ; i++){
					String nh = line.substring(startIdx,endIdx);
					int houses = Integer.parseInt(nh);
					mw.put(new IntWritable(i), new IntWritable(houses));
					startIdx += 9;
					endIdx += 9;
					
					if (pCnt < pLIMIT) {
						System.out.println("[MAP] :" + state + "\t# rooms : " + (i+1) + "\t# houses : " + houses);
						pCnt++;
					}
				}
				context.write(new Text(state), mw);
			}
		}
	}
}

