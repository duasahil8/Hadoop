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

import sample.YearsCourseOffered.IntSumReducer;
import sample.YearsCourseOffered.TokenizerMapper;

public class DepartmentInceptionYear {
	



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
				
				String semesterName = lineSplit[3];

				String department = lineSplit[4];
			//	String subject = lineSplit[8];
				int enrollment = Integer.valueOf(lineSplit[9]);
				if(enrollment > 0){
					String sem[] = semesterName.split(" ");
					String search = department + "_"  + sem[1];
					
						//temp.put(search, sem[1]);
						IntWritable v = new IntWritable(Integer.valueOf(sem[1]));
						String keyword = department;
						word.set(keyword);
						context.write(word, v);
						
					
					
				
					
					
 							
							
						

					
				
					
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
			int min = 2018;
		//	int max = -1;
			int diff;

			for (IntWritable val : values) {

				//if(val.get() > max) max = val.get();
				if(val.get() < min){
					min = val.get();
				}
				
				

			}
		////	System.out.print(key + ":");
			//System.out.print(max + " ");
			//System.out.println(min + " ");
			//System.out.println(max-min + " ");
			
			
			/*
			diff = max-min;
			String res = String.valueOf(diff);*/
			Text key1 = new Text();
			//key1.set(key.toString() + "from" + String.valueOf(min) + "to" + String.valueOf(max)); 
			
			result.set(min);
			context.write(key, result);
			

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(DepartmentInceptionYear.class);

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
