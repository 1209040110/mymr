package ch01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {
	private IntWritable first;
	private IntWritable second;
	public IntPair(){
		set(new IntWritable(),new IntWritable());
	}
	public IntPair(int first,int second){
		set(new IntWritable(first),new IntWritable(second));
	}
	public IntPair(IntWritable first,IntWritable second){
		set(first,second);
	}
	public void set(IntWritable first,IntWritable second){
		this.first=first;
		this.second=second;
	}

	public IntWritable getFirst() {
		return first;
	}

	public IntWritable getSecond() {
		return second;
	}

	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	//序列化
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public int hashCode() {
		return first.hashCode()+second.hashCode();
	}
	@Override
	public boolean equals(Object o) {
		if(o instanceof IntPair){
			IntPair ip=(IntPair)o;
			return first.equals(ip.first)&&second.equals(ip.second);
		}
		return false;
	}
	@Override
	public String toString() {
		return first+"\t"+second;
	}
	@Override
	public int compareTo(IntPair ip) {
		int cmp=first.compareTo(ip.first);
		if(cmp !=0){
			return cmp;
		}
		return second.compareTo(ip.second);
	}
	
	public static int compare(IntWritable i1,IntWritable i2){
		return i1.compareTo(i2);
		
	}
}
