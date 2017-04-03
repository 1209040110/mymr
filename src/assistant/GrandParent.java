package assistant;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class GrandParent {

	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString();
			String child=line.split(" ")[0];
			String parent=line.split(" ")[1];
			context.write(new Text(child),new Text("+"+parent));
			context.write(new Text(parent),new Text("-"+child));
		}
	}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			List<Text> grandChilds=new ArrayList<Text>();
			List<Text> grandParents=new ArrayList<Text>();
			for(Text t:values){
				String s=t.toString();
				if(s.startsWith("+")){
					grandParents.add(new Text(s.substring(1)));
				}
				else if(s.startsWith("-")){
					grandChilds.add(new Text(s.substring(1)));
				}
			}
			
			int i,j;
			for(i=0;i<grandChilds.size();i++){
				for(j=0;j<grandParents.size();j++){
					context.write(new Text(grandChilds.get(i)),new Text(grandParents.get(j)));
				}
			}
		}
	}
	/*
	public static class GrandParentTextpair implements WritableComparable<GrandParentTextpair>{
		private Text first;
		private IntWritable second;
		public GrandParentTextpair(String first,int second){
			this.first=new Text(first);
			this.second=new IntWritable(second);
		}
		public Text getFirst() {
			return first;
		}
		
		public void setFirst(Text first) {
			this.first = first;
		}
		public IntWritable getSecond() {
			return second;
		}
		public void setSecond(IntWritable second) {
			this.second = second;
		}
		@Override
		public int hashCode() {
			return first.hashCode()*163+second.get();
		}
		@Override
		public boolean equals(Object obj) {
			if(! (obj instanceof GrandParentTextpair))
				return false;
			else{
				GrandParentTextpair gtp=(GrandParentTextpair)obj;
				return this.getFirst().equals(gtp.first)&&this.getSecond().get()==gtp.getSecond().get();
			}
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			first.write(out);
		}
		@Override
		public String toString() {
			return first+"\t"+second;
		}
		@Override
		public int compareTo(GrandParentTextpair gtp) {
			return 0;
		}
		
	}
	*/
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: GrandParent <in> <out>");
	      System.exit(2);
	    }
	    
		Job job=new Job(conf,"GrandParent");
		job.setJarByClass(GrandParent.class);
		
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
