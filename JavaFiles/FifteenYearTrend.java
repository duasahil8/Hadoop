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



public class FifteenYearTrend {


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
				String sem[] = semesterName.split(" ");
				String department = lineSplit[4];
				String subject = lineSplit[8];
				int enrollment = Integer.valueOf(lineSplit[9]);
				int year = Integer.parseInt(sem[1]);
				 
				
				
				if(enrollment > 0 && enrollment >10){
					IntWritable v = new IntWritable(enrollment);
					if(year>=2011 && year<=2015){
						String keyword = department + "_" + "2011 to 2015" ;
						word.set(keyword);
						context.write(word, v);
					}
					if(year>=2006 && year<=2010){
						String keyword = department + "_" + "2006 to 2010"  ;
						word.set(keyword);
						context.write(word, v);
					}
					if(year>=2000 && year<=2005){
						String keyword = department + "_" +  "2000 to 2005";
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
			result.set(avg);

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(FifteenYearTrend.class);

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


