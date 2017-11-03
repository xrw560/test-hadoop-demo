package com.ncut.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestAppend {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.138.50:8020");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream outputStream = fs.append(new Path("/beifeng/api/createNewFile1.txt"));
		outputStream.write("你好，美女".getBytes());
		outputStream.close();
		fs.close();
	}
}
