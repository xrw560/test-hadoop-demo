package com.ncut.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;

import com.ncut.hdfs.utils.HdfsUtil;

public class TestOpen {

	public static void main(String[] args) throws IOException {
		test1();
	}

	private static void test1() throws IOException {
		//hdfs dfs -cat
		FileSystem fs = HdfsUtil.getFileSystem();
		InputStream in = fs.open(new Path("/beifeng/api/1.txt"));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}

		br.close();
		in.close();
		fs.close();

	}
}
