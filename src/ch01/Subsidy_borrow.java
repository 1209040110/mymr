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


public class Subsidy_borrow {
	public static class Subsidy_borrowMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key, Text value, Context context) 
				throws java.io.IOException ,InterruptedException {
			String line=value.toString();
			String stuNo=line.split(",")[0];
			context.write(new Text(stuNo),new IntWritable(1));
		}
		
	}
	public static class Subsidy_borrowReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values){
				sum+=1;
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("");
	      System.exit(2);
	    }
	    
		Job job=new Job();
		job.setJarByClass(Subsidy_borrow.class);
		
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		
		job.setMapperClass(Subsidy_borrowMapper.class);
		job.setCombinerClass(Subsidy_borrowReducer.class);
		job.setReducerClass(Subsidy_borrowReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
