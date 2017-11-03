package com.ncut.hdfs.utils;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FsAclPermission;

public class HdfsUtil {

	public static Configuration getConfiguration() {
		Configuration conf = new Configuration();
		// 设置集群位置信息
		conf.set("fs.defaultFS", "hdfs://192.168.138.50:8020");
		return conf;
	}

	public static FileSystem getFileSystem() throws IOException {
		FileSystem fs = FileSystem.get(getConfiguration());
		return fs;
	}

	public static FileSystem getFileSystem(Configuration conf) throws IOException {
		return FileSystem.get(conf);
	}

	public static boolean deleteFile(String path) throws Exception {
		FileSystem fs = null;
		try {
			fs = getFileSystem();
			return fs.delete(new Path(path), true);
		} finally {
			if (fs != null) {
				fs.close();
			}
		}

	}

}
