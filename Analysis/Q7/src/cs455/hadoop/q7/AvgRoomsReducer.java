package cs455.hadoop.q7;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

public class AvgRoomsReducer extends Reducer<Text, MapWritable, Text, Text> {
	private final static int NUM_STATES = 51;  // Not considering PR and VI for this
	LinkedList<Float> avgRoomsToSort = new LinkedList<Float>();
	
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int[] perRoomHouseTotal = new int[9];
		float avgRoomsPerHousePerState;
		int numRoomsTotalPerState = 0;
		int numHousesTotalPerState = 0;
		int size = 9;
		int index = 0;
		
		// For each state calculate the total number of houses based on the 
		// number of rooms present per house. In other words calculate total
		// number of houses having 1 room, 2 rooms, 3 rooms etc
		for (MapWritable value : values ) {
			 for(int i = 0; i < 9; i++) {
				int perBlockHouseTotal = ((IntWritable) value.get(new IntWritable(i))).get();
				perRoomHouseTotal[i] += perBlockHouseTotal;
			}
		}
		
		for (int i = 0; i < 9; i++) {
			// Calculate the avg num of rooms per state 
			numRoomsTotalPerState += perRoomHouseTotal[i] * (i+1);
			numHousesTotalPerState += perRoomHouseTotal[i];
		}
		avgRoomsPerHousePerState = (float) numRoomsTotalPerState / (float) numHousesTotalPerState;
		avgRoomsToSort.add(avgRoomsPerHousePerState);
		System.out.println(key + " :\t#houses : " + numHousesTotalPerState + "\t#rooms : " + numRoomsTotalPerState +
							"\tavg : " + avgRoomsPerHousePerState + "\tso far.. " + avgRoomsToSort.size());
		
		if(avgRoomsToSort.size() >= NUM_STATES){
			Collections.sort(avgRoomsToSort);
			index = (int) Math.round((0.95) * NUM_STATES);

			System.out.println("Ans: " + avgRoomsToSort.get(index-1));
			
			context.write(new Text(new String("95% of homes have")), new Text(new String(avgRoomsToSort.get(index-1) + " rooms or less")));
		}
	}
}
