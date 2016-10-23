//Team members - Vaibhav Sharma 50169905 ; Sahil Dua 50170314

package sample;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DayTrends {

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
		
		
		private Text word = new Text();

		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {
			

			String newLine = value.toString();

			String[] lineSplit = newLine.split(",");
			
			if (lineSplit.length <= 11) {
				String semesterName = lineSplit[3];
				String sem[] = semesterName.split(" ");
				String year = sem[1];
				
				String days = lineSplit[6];
				if(days.equals("M-F")) days = "MTWRF";
				if(days.equals("M-S")) days = "MTWRFS";
				if( ! (days.contains("ARR") || days.contains("UNKWN"))){
					for(int i=0; i<days.length(); i++){
						//System.out.println("days" + days.;
						//System.out.println("My Key: " + day.valueOf(String.valueOf(days.charAt(i))));
						String keyWord="";
						if(i<days.length()-1 && days.charAt(i+1) == 'H'){
							keyWord = year+"_"+day.valueOf("R");
							i = i+1;
						}
						else{
							keyWord = year + "_" + day.valueOf(String.valueOf(days.charAt(i)));
						}
						IntWritable v = new IntWritable(1);
						

						word.set(keyWord.trim());

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

		job.setJarByClass(DayTrends.class);

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
