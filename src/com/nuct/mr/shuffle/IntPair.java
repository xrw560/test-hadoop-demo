package com.nuct.mr.shuffle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义输出Key数据类型
 *
 */
public class IntPair implements WritableComparable<IntPair>{
	
	private int first;
	private int second;
	
	

	public IntPair() {
		super();
	}

	
	public IntPair(int first, int second) {
		super();
		this.first = first;
		this.second = second;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.first);
		out.writeInt(this.second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.first = in.readInt();
		this.second = in.readInt();
	}

	@Override
	public int compareTo(IntPair o) {
		if(o==this){
			return 0;
		}
		//先按照first排序
		int tmp = Integer.compare(this.first, o.first);
		if(tmp!=0){
			return tmp;
		}
		//再按second排序
		tmp = Integer.compare(this.second, o.second);
		return tmp;
	}


	public int getFirst() {
		return first;
	}


	public void setFirst(int first) {
		this.first = first;
	}


	public int getSecond() {
		return second;
	}


	public void setSecond(int second) {
		this.second = second;
	}
	

}
