package assistant;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class AdvancedInvertedIndex {
	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString();
			FileSplit fileSplit=(FileSplit) context.getInputSplit();
			System.out.println(fileSplit.toString());
			String fileName=fileSplit.getPath().getName();
			Text word=new Text();
			String[] words=line.split(" ");
			Text fileName_lineOffset=new Text(fileName+"@"+key.toString());
			int i;
			for(i=0;i<words.length;i++){
				word.set(words[i]);
				context.write(word, fileName_lineOffset);
			}
		}
	}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer sb=new StringBuffer();
			boolean f=false;
			for(Text docId:values){
				if(f) sb.append(";");
				sb.append(docId.toString());
				f=true;
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    
		Job job=new Job(conf,"AdvancedInvertedIndex");
		job.setJarByClass(AdvancedInvertedIndex.class);
		
		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.13.30.72/input/doc1"));
		FileInputFormat.addInputPath(job, new Path("hdfs://10.13.30.72/input/doc2"));
		FileInputFormat.addInputPath(job, new Path("hdfs://10.13.30.72/input/doc3"));
		
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.13.30.72/inveredIndex/Indexoutput4"));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
