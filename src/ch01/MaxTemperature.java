package ch01;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MaxTemperature {
	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
		public static final int MISSING=9999;
		@Override
		public void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line=value.toString();
			String year=line.substring(15,19);
			int airTemperature;
			if(line.charAt(87)=='+'){
				airTemperature=Integer.parseInt(line.substring(88,92));
			}else{
				airTemperature=Integer.parseInt(line.substring(87,92));
			}
			String quality=line.substring(92,93);
			if(airTemperature!=MISSING&&quality.matches("[01459]")){
				context.write(new Text(year),new IntWritable(airTemperature));
			}
		}
	}
	
	public static class MaxTemperatureReducer extends  Reducer<Text, IntWritable, Text,IntWritable>
	{

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int result=Integer.MIN_VALUE;
			for(IntWritable value:values){
				result=Math.max(result,value.get());
			}
			context.write(key, new IntWritable(result));
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    
		Job job=new Job(conf,"MaxTemperature");
		job.setJarByClass(MaxTemperature.class);
		
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
