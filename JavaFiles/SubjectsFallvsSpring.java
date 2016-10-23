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

import sample.CoursesPerDepartmentYearwise.IntSumReducer;
import sample.CoursesPerDepartmentYearwise.TokenizerMapper;

public class SubjectsFallvsSpring {

	



	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();
		
		private static Map<String,Integer> temp = new HashMap<String,Integer>();
		
		

		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {

			String newLine = value.toString();

			String[] lineSplit = newLine.split(",");
			
			if (lineSplit.length <= 11) {
				
				String semesterName = lineSplit[3];

				String department = lineSplit[4];
				String subject = lineSplit[8];
				int enrollment = Integer.valueOf(lineSplit[9]);
				if((semesterName.contains("Spring")||semesterName.contains("Fall"))&& enrollment > 0){
					String sem[] = semesterName.split(" ");
					String search ="";
					if(semesterName.contains("Spring")) search = "Fall";
					else search = "Spring";
					
					
					
					String keyWord = department + "_"  + sem[1] + "_" + subject + "_" + search;
					String keyword1 = department + "_"  + sem[1] + "_" + subject + "_" + sem[0] ;
					
					if(temp.containsKey(keyWord)){
						
						int enroll = temp.get(keyWord);
						IntWritable v = new IntWritable(enroll);
						word.set(keyWord.trim());

						context.write(word, v);
						 v = new IntWritable(enrollment);
						word.set(keyword1);
						context.write(word, v);
					}
					else{
						if(temp.containsKey(keyword1) && temp.get(keyword1)< enrollment){
							temp.put(keyword1, enrollment);
						}
						if(! temp.containsKey(keyword1)){
							temp.put(keyword1, enrollment);
						}
						
					}
					
				
					
				}
				
				//cse_Fall 2011_IntroML
				
					
					
					

				
					
				

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

			for (IntWritable val : values) {

				sum += val.get();
				if(val.get() > max){
					max = val.get();
				}

			}
			

			result.set(max);

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(SubjectsFallvsSpring.class);

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
