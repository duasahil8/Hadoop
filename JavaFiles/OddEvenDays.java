package sample;

import java.io.IOException;
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



public class OddEvenDays {


	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();
		
		private static Map<String,String> temp = new HashMap<String,String>();
		
		

		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {

			String newLine = value.toString();

			String[] lineSplit = newLine.split(",");
			
			if (lineSplit.length <= 11) {
				
				String days = lineSplit[6];
				String department = lineSplit[4];
				String hall = lineSplit[5];
				//cse_MoWeFr , cse_TuThSa
				int maxAllowed = Integer.valueOf(lineSplit[10]);
				 
				if(!days.startsWith("ARR")&&!days.startsWith("UNK")){
						IntWritable v = new IntWritable(1);
					 	if(days.contains("M")||days.contains("W")||days.contains("F")){
					 		String keyword = department + "_" + "MoWeFr";
					 		word.set(keyword);
							context.write(word, v);
					 	}
					 	else if(days.contains("T")||days.contains("R")||days.contains("S")){
					 		String keyword = department + "_" + "TuThSa";
					 		word.set(keyword);
							context.write(word, v);
					 	}
						  
			}
			}
			}
		 
	}

	public static class IntSumReducer

	extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,

				Context context

				) throws IOException, InterruptedException {

			int sum = 0;
			int count = 0 ; 
			for (IntWritable val : values) {
				count++;
				sum += val.get();
				 
			}
			
			int avg = sum/count;
			
			result.set(sum);

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(OddEvenDays.class);

		job.setMapperClass(TokenizerMapper.class);

		job.setCombinerClass(IntSumReducer.class);

		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}







}