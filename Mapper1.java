package bigData;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text word = new Text();
	IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String file = value.toString();
		String[] hlines = file.split(",");
		if (hlines[0].equals("Year"))
			return;
		String origin_Destination = hlines[7] + "-" + hlines[8];
		word.set(origin_Destination);
		context.write(word, one);
	}
}