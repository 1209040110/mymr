package assistant;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//倒排索引
public class InvertedIndex {
	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString();
			String[] words=line.split(" ");
			int i;
			Text docId=new Text(words[0]);
			for(i=1;i<words.length;i++){
				context.write(new Text(words[i]) , docId);
			}
		}
	}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer sb=new StringBuffer();
			Boolean flag=false;
			for(Text docId:values){
				if(flag)
					sb.append(",");
				sb.append(docId.toString());
				flag=true;
				
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: InvertedIndex <in> <out>");
	      System.exit(2);
	    }
	    
		Job job=new Job(conf,"InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
