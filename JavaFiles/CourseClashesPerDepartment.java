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
import org.apache.hadoop.util.hash.Hash;

import sample.DayTrends.IntSumReducer;
import sample.DayTrends.TokenizerMapper;
import sample.DayTrends.day;

public class CourseClashesPerDepartment {


	 public static enum day{
		 M ("Monday"), T("Tuesday"),W("Wednesday"), R("Thursday"), F("Friday"), S("Saturday");
		   private final String text;
		   private day(final String text) {
		        this.text = text;
		    }

		    /* (non-Javadoc)
		     * @see java.lang.Enum#toString()
		     */
		    @Override
		    public String toString() {
		        return text;
		    }
	 }
	
	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		
		private final Map temp = new HashMap() ;
			private Text word = new Text();

		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {
			

			String newLine = value.toString();

			String[] lineSplit = newLine.split(",");
			
			if (lineSplit.length <= 11) {
				String semesterName = lineSplit[3];
				String sem[] = semesterName.split(" ");
				String year = sem[1];
				String department = lineSplit[4];
				
				String days = lineSplit[6];
				
				String time = lineSplit[7];
				String subject = lineSplit[8];
				if(days.equals("M-F")) days = "MTWRF";
				if(days.equals("M-S")) days = "MTWRFS";
				if( ! (days.contains("ARR") || days.contains("UNKWN"))){
					for(int i=0; i<days.length(); i++){
						//System.out.println("days" + days.;
						//System.out.println("My Key: " + day.valueOf(String.valueOf(days.charAt(i))));
						String keyWord="";
						if(i<days.length()-1 && days.charAt(i+1) == 'H'){
							//keyWord = year+"_"+day.valueOf("R");
							keyWord = department+ "_" + semesterName + "_" + day.valueOf("R") + "_" + time;
							
							i = i+1;
						}
						else{
						//	keyWord = year + "_" + day.valueOf(String.valueOf(days.charAt(i)));
							keyWord = department+ "_" + semesterName + "_" + day.valueOf(String.valueOf(days.charAt(i)))
														+ "_" + time;
						}
						
						if(! temp.containsKey(keyWord)){
							temp.put(keyWord, subject);
							IntWritable v = new IntWritable(1);
							word.set(keyWord.trim());

							context.write(word, v);
						}
						else{
							if(! temp.get(keyWord).equals(subject)){
								IntWritable v = new IntWritable(1);
								
								temp.put(keyWord, subject);
								word.set(keyWord.trim());

								context.write(word, v);
							}
						}
						
							

						
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

			for (IntWritable val : values) {

				sum += val.get();

			}
			if(sum > 1){
				result.set(sum-1);

				context.write(key, result);

			}

			
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
