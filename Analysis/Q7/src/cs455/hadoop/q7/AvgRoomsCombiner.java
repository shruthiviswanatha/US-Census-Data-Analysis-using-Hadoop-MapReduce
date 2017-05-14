package cs455.hadoop.q7;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AvgRoomsCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int[] perRoomHouseTotal = new int[9];
		MapWritable mw = new MapWritable();
		
		
		// For each state calculate the total number of houses based on the 
		// number of rooms present per house. In other words calculate total
		// number of houses having 1 room, 2 rooms, 3 rooms etc
		for (MapWritable value : values ) {
			for (int i = 0; i < 9; i++) {
				int perBlockHouseTotal =  ((IntWritable) value.get(new IntWritable(i))).get();
				perRoomHouseTotal[i] += perBlockHouseTotal;
			}
		}
		
		// Store partial house total (based on # of rooms) in a map to be fed to the reducer
		for (int i = 0; i < 9; i++) {
			mw.put(new IntWritable(i), new IntWritable(perRoomHouseTotal[i]));
			System.out.println("[COM] " + key + "\t# rooms : " + (i+1) + "\t# houses : " + perRoomHouseTotal[i]);
		}
		
		context.write(key, mw);
	}
}
