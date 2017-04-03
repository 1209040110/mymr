package assistant;

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
import org.apache.hadoop.util.GenericOptionsParser;



public class Sort {

	public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
		private static IntWritable data=new IntWritable();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString();
			data.set(Integer.parseInt(line)); 
			context.write(data, new IntWritable(1));
		}
	}
	public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		private static IntWritable linenum=new IntWritable(1);
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			for(IntWritable val:values){
				context.write(linenum, key);
				linenum.set(linenum.get()+1);
			}
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 4) {
	      System.err.println("Usage: Sort <in1> <in2> <in3> <out>");
	      System.exit(2);
	    }
	    
		Job job=new Job(conf,"Sort");
		job.setJarByClass(Sort.class);
		
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job,new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job,new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[3]));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
