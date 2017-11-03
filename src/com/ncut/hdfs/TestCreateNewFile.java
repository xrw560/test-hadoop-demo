package com.ncut.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestCreateNewFile {

	public static void main(String[] args) throws Exception {
		test1();
		test2();
	}

	/**
	 * 指定绝对路径
	 * 
	 * @throws IOException
	 */
	private static void test1() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.138.50:8020");
		FileSystem fs = FileSystem.get(conf);
		boolean createNewFile = fs.createNewFile(new Path("/beifeng/api/createNewFile1.txt"));
		System.out.println(createNewFile ? "创建成功" : "创建失败");
		fs.close();
	}

	private static void test2() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.138.50:8020");
		FileSystem fs = FileSystem.get(conf);
		boolean created = fs.createNewFile(new Path("api/createNewFile2.txt"));
		System.out.println(created ? "创建成功" : "创建失败");
		fs.close();
	}

}
