package bigData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PopularAirports {
	public static void main(String[] args) throws Exception {

		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1);
		job1.setJarByClass(PopularAirports.class);
		job1.setJobName("Origin and Destination Airports");

		FileInputFormat.addInputPath( job1, new Path("input"));
	
		FileOutputFormat.setOutputPath(job1, new Path("output" + "/temp/tempAirports"));

		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		Configuration conf2 = new Configuration();

		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(PopularAirports.class);
		job2.setJobName("Origin Destination Airport pairs:");

		FileInputFormat.setInputPaths(job2, new Path("output" + "/temp/tempAirports"));
		FileOutputFormat.setOutputPath(job2, new Path("output" + "/TopAirports"));

		job2.setMapperClass(OriginAndDestAirportsMapper.class);
		job2.setReducerClass(OriginAndDestAirportsReducer.class);

		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}