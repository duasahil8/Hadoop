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



public class RoomUtilization {
	



	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {

			String newLine = value.toString();

			String[] lineSplit = newLine.split(",");
			if (lineSplit.length <= 9) {
				String room = lineSplit[2];

				String enrollment = lineSplit[7];
				String capacity = lineSplit[8];
				int enroll = Integer.parseInt(enrollment);
				int cap = Integer.parseInt(capacity);
				if(cap !=0){
					int util = (int) (((float)enroll/cap)*100);
					

					String semesterName = lineSplit[1];
					String year[] = semesterName.split(" ");
					if(util < 100 && util > 0){
						if(!(room.contains("Unknown") || room.contains("ARR"))){
							IntWritable v = new IntWritable(util);

							String keyWord = room + "_" + year[1];

							word.set(keyWord.trim());

							context.write(word, v);
						}
					
					}
					
					
					
					//int val = Integer.valueOf(lineSplit[7]);
					

					
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
			avg = sum/count;

			result.set(avg);
		

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(RoomUtilization.class);

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
