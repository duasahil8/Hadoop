package sample;

import java.io.IOException;

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

import sample.SpringvsFall_department.IntSumReducer;
import sample.SpringvsFall_department.TokenizerMapper;

public class HighLowDepartment {


	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {

			String newLine = value.toString();

			String[] lineSplit = newLine.split(",");
			
			if (lineSplit.length <= 11) {
				String semesterName = lineSplit[3];

				String department = lineSplit[4];

				

				

				int enrollment = Integer.valueOf(lineSplit[9]);
				String range = "";
				if(enrollment < 40) range = "Low";
				if(enrollment >= 40 && enrollment < 80) range = "Medium";
				if(enrollment >= 80) range = "High";

				if(enrollment >0){
					
					String sem[] = semesterName.split(" ");
					IntWritable v = new IntWritable(1);
					String keyWord = department + "_"  + sem[1] + "_" + range;
					
					word.set(keyWord.trim());

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

			for (IntWritable val : values) {

				sum += val.get();

			}

			result.set(sum);

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(HighLowDepartment.class);

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
