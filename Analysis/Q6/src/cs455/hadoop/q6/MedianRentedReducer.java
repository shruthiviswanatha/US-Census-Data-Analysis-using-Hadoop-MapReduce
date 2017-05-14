package cs455.hadoop.q6;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MedianRentedReducer extends Reducer<Text, MapWritable, Text, Text> {
	static final String finValues[] = {
										"Less than $100", 
										"$100 - $149", 
										"$150 - $199",
										"$200 - $249",
										"$250 - $299",
										"$300 - $349",
										"$350 - $399",
										"$400 - $449",
										"$450 - $499",
										"$500 - $549",
										"$550 - $599",
										"$600 - $649",
										"$650 - $699",
										"$700 - $749",
										"$750 - $999",
										"$1000 or more"};
	
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
		int[] arr = new int[17];
		int num_grps = 17;
		int total = 0;
		int median_idx = 0;
		int runTotal = 0;
		int k = 0;
		
		for (MapWritable value : values ) {
			for(int i = 0; i<num_grps; i++){
				int v = ((IntWritable) value.get(new IntWritable(i))).get();
				arr[i] += v;
			}
		}
		for(int j = 0; j < num_grps; j++ ){
			System.out.println(key.toString() + " : " + arr[j]);
			total += arr[j];
		}
		boolean odd = false;
		if(total % 2 > 0){
			odd = true;
		}
		
		median_idx = odd ? (total/2) + 1 : total/2 + 1;
		for(k=1; k<num_grps; k++){
			runTotal += arr[k];
			if(median_idx < runTotal){
				break;
			} 
		}
		if(k==num_grps){
			System.out.println("Error");
		}
		
		context.write(key,new Text(new String("The median value of a house occupied by the Renters is :" +finValues[k])));
	}
}
