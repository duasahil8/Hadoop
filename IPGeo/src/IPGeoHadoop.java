import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class IPGeoHadoop {


	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, Text> { 
		
		
		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {

			String newLine = value.toString();
			String[] lineSplit = newLine.split(",");
			String k = "", v = "";
			if(lineSplit.length==2){
				k = lineSplit[0];
				v = lineSplit[1];
				context.write(new Text(k), new Text(v)); 
			}
			else if (lineSplit.length==3){
				k = lineSplit[2];
				v = lineSplit[0] + ", " + lineSplit[1];
				context.write(new Text(k), new Text(v)); 
			} 
			
			
		} 
		 
	}

	public static class IntSumReducer

	extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,

				Context context

			) throws IOException, InterruptedException {
			 
		
			String res = "";
			ArrayList<String> list = new ArrayList<String>(); 
			
			for(Text b : values) {
				res += b.toString() + " ";
				list.add(b.toString());
			}
			
			Collections.sort(list);
			
	
			context.write(key, new Text(list.toString()));
			
			 
	}
}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(IPGeoHadoop.class);

		job.setMapperClass(TokenizerMapper.class);

		job.setCombinerClass(IntSumReducer.class);

		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}