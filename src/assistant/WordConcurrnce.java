package assistant;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class WordConcurrnce {
	public static class MyMapper extends Mapper<Object,Text,WordPair,IntWritable>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString();
			String[] words=line.split(" ");
			int i;
			for(i=0;i<words.length-1;i++){
				context.write(new WordPair(words[i], words[i+1]), new IntWritable(1));
			}
			
		}
	}
	public static class MyReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable>{
		public void reduce(WordPair key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			context.write(key, new IntWritable(sum));
			
		}
	}
	public static class WordPair implements WritableComparable<WordPair>{
		private Text first;
		private Text second;
		public WordPair() {
			first=new Text();
			second=new Text();
		}
		public Text getFirst() {
			return first;
		}
		public void setFirst(Text first) {
			this.first = first;
		}
		public Text getSecond() {
			return second;
		}
		public void setSecond(Text second) {
			this.second = second;
		}
		public WordPair(String first,String second) {
			// TODO Auto-generated constructor stub
			this.first=new Text(first);
			this.second=new Text(second);
		}
		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return first.hashCode()*163+second.hashCode();
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return first+"\t"+second;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}
		@Override
		public boolean equals(Object o) {
			if(o instanceof WordPair){
				WordPair wp=(WordPair)o;
				return wp.first.equals(first)&&wp.second.equals(second);
			}
			return false;
		}
		@Override
		public int compareTo(WordPair wp) {
			int cmp=this.first.compareTo(wp.first);
			if(cmp!=0){
				return cmp;
			}
			return this.second.compareTo(wp.second);
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
	
	    
		Job job=new Job(conf,"WordConcurrnce");
		job.setJarByClass(WordConcurrnce.class);
		
		
		FileInputFormat.addInputPath(job,new Path("C:\\Users\\yichen\\Desktop\\wordconcurrency.txt"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://10.13.30.72/user/yichen/output/wordconcurrencyoutput"));
		
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyReducer.class);
		
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
