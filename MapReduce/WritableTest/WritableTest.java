package com.xfyan.MR.one;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WritableTest implements WritableComparable<WritableTest>{
	private Text first;
	private Text second;
	
	public void set(Text first,Text second){
		this.first = first;
		this.second = first;
	}
	
	public WritableTest(String first,String second){
		set(new Text(first),new Text(second));
	}
	
	public WritableTest(Text first,Text second){
		set(first,second);
	}
	
	public	WritableTest(){
		set(new Text(),new Text());
	}
	
	public Text getFirst(){
		return first;
	}
	
	public Text getSecond(){
		return second;
	}
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public int compareTo(WritableTest o) {
		int cmp = first.compareTo(o.first);
		if(cmp != 0){
			return cmp;
		}
		
		return second.compareTo(o.second);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof WritableTest){
			WritableTest wt = (WritableTest)obj;
			return first.equals(wt.first) && second.equals(wt.second); 
		}
		
		return false;
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public String toString() {
		return first + "\t" + second;
	}
	
}
