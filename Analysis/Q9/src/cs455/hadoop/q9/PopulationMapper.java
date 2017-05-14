package cs455.hadoop.q9;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PopulationMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		String slevel = line.substring(10, 13);
		int sumlevel = Integer.parseInt(slevel);
		if(sumlevel == 100){
			String seg = line.substring(24,28);
			int segment = Integer.parseInt(seg);
			if(segment == 1) {
				String state = line.substring(8, 10);
				String p = line.substring(300,309);
				int persons = Integer.parseInt(p);
				
				context.write(new Text(state), new IntWritable(persons));
			}
		}
	}
}