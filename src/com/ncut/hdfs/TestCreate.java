package com.ncut.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestCreate {

	public static void main(String[] args) throws Exception {
		test2();
	}

	private static void test1() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.138.50:8020");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path("/beifeng/api/1.txt"));
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
		writer.write("智能交通");
		writer.newLine();
		writer.write("离线数据 分析平台");

		writer.close();
		out.close();
		fs.close();
	}

	private static void test2() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.138.50:8020");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path("/beifeng/api/2.txt"), (short) 1);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
		writer.write("智能交通");
		writer.newLine();
		writer.write("离线数据 分析平台");

		writer.close();
		out.close();
		fs.close();
	}
}
