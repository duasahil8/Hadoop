package sample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sample.SubjectAverageEnrollment.IntSumReducer;
import sample.SubjectAverageEnrollment.TokenizerMapper;

public class MostPopularCourseByYear{ 

	



	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {

			String newLine = value.toString();

			String[] lineSplit = newLine.split(",");
			if (lineSplit.length <= 11) {String department = lineSplit[4];
			String courseName = lineSplit[8];
			String enrollment = lineSplit[9];
			String semesterName = lineSplit[3];
			String sem[] = semesterName.split(" ");
			String year = sem[1];
			
			
			
			//int val = Integer.valueOf(lineSplit[7]);
				
					IntWritable v = new IntWritable(Integer.parseInt(enrollment));

					String keyWord = department + "_" + courseName + "_" + year;

					word.set(keyWord.trim());

					context.write(word, v);
				
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
			int max = -1;
			int count = 0;
			int avg ;
			

			for (IntWritable val : values) {

				sum += val.get();
				count = count +1;
				if(val.get() >max){
					max = val.get();
				}

			}
			//avg = sum/count;

			//result.set(avg);
		

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(WordCount.class);

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
