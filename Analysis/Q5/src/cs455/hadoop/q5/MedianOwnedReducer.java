package cs455.hadoop.q5;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MedianOwnedReducer extends Reducer<Text, MapWritable, Text, Text> {
	static final String finValues[] = {"Less than $15000", 
										"$15000 - $19999", 
										"$20000 - $24999",
										"$25000 - $29999",
										"$30000 - $34999",
										"$35000 - $39999",
										"$40000 - $44999",
										"$45000 - $49999",
										"$50000 - $59999",
										"$60000 - $74999",
										"$75000 - $99999",
										"$100000 - $124999",
										"$125000 - $149999",
										"$150000 - $174999",
										"$175000 - $199999",
										"$200000 - $249999",
										"$250000 - $299999",
										"$300000 - $399999",
										"$400000 - $499999",
										"$500000 or more"};
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int[] arr = new int[20];
		int num_grps = 20;
		int total = 0;
		int median_idx = 0;
		int runTotal = 0;
		int k = 0;
		//particular range total houses
		for (MapWritable value : values ) {
			for(int i = 0; i<num_grps; i++){
				int v = ((IntWritable) value.get(new IntWritable(i))).get();
				arr[i] += v;
			}
		}
		//particular state total houses
		for(int j = 0; j < num_grps; j++ ){
			System.out.println(key.toString() + " : " + arr[j]);
			total += arr[j];
		}
		boolean odd = false;
		if(total % 2 > 0){
			odd = true;
		}
		
		median_idx = odd ? (total/2) + 1 : total/2;
		for(k=0; k<num_grps; k++){
			runTotal += arr[k];
			if(median_idx < runTotal){
				break;
			} 
		}
		if(k==num_grps){
			System.out.println("Error");
		}
		
		context.write(key,new Text(new String("The median value of a house occupied by the owners is :" +finValues[k])));
	}
}
