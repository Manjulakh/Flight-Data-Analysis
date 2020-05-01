package bigData;


import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		context.write(key, new IntWritable(total));
	}
}