package ch01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySort2 extends Configured implements Tool {

	public static class SecondarySortMapper extends Mapper<LongWritable,Text,IntPair,IntWritable>{
		
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			int first=Integer.parseInt(line.split(" ")[0]);
			int second=Integer.parseInt(line.split(" ")[1]);
			context.write(new IntPair(first, second),new IntWritable(second));
		}
	}
	
	static class SecondarySortRecuder extends Reducer<IntPair,IntWritable,IntWritable,IntWritable>{
		
		protected void reduce(IntPair key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			for(IntWritable value:values){
				context.write(key.getFirst(), value);
			}
		}
	}
	public static class FirstPartitioner extends Partitioner<IntPair,IntWritable>{

		@Override
		public int getPartition(IntPair key, IntWritable value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)%numPartitions;
		}
		
	}
	public static class KeyComparator extends WritableComparator{
		protected KeyComparator(){
			super(IntPair.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPair ip1=(IntPair)a;
			IntPair ip2=(IntPair)b;
			int cmp=IntPair.compare(ip1.getFirst(),ip2.getFirst());
			if(cmp!=0){
				return cmp;
			}
			return IntPair.compare(ip1.getSecond(), ip2.getSecond());
		}
		
	}
	
	public static class GroupComparator extends WritableComparator{
		protected GroupComparator(){
			super(IntPair.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPair ip1=(IntPair)a;
			IntPair ip2=(IntPair)b;
			return IntPair.compare(ip1.getFirst(), ip2.getFirst());
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		
		if(args.length<2){
			System.out.println("secondarysort2 <inDir> <outDir>");
		    ToolRunner.printGenericCommandUsage(System.out);
		    return -1;
		}
			Job job=new Job(getConf(),"SecondarySort2 job");
			job.setJarByClass(SecondarySort2.class);
		
			
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapOutputKeyClass(IntPair.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setMapperClass(SecondarySortMapper.class);
			job.setPartitionerClass(FirstPartitioner.class);
			job.setSortComparatorClass(KeyComparator.class);
			job.setGroupingComparatorClass(GroupComparator.class);
			job.setReducerClass(SecondarySortRecuder.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(1);//important
			
			return job.waitForCompletion(true)?0:1;
		
		
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new SecondarySort2(), args);
		System.exit(exitCode);
	}

}
