package com.ncut.hdfs.tests;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.ncut.hdfs.utils.HdfsUtil;

public class TestHdfs {

	@Test
	public void testMkdirs() throws IOException {
		FileSystem fs = HdfsUtil.getFileSystem();
		boolean mkdirsed = fs.mkdirs(new Path("/beifeng/api/mkdirs"));
		System.out.println(mkdirsed ? "创建成功" : "创建失败");
		fs.close();
	}

	@Test
	public void testCopyFromLocal() throws IOException {
		FileSystem fs = HdfsUtil.getFileSystem();
		fs.copyFromLocalFile(new Path("D://te.txt"), new Path("/beifeng/api/3.txt"));
		fs.close();
	}

	@Test
	public void testCopyToLocal() throws IOException {
		FileSystem fs = HdfsUtil.getFileSystem();
		fs.copyToLocalFile(new Path("/beifeng/api/3.txt"), new Path("D://4.txt"));
		fs.close();
	}

	@Test
	public void testDelete() throws IOException {
		FileSystem fs = HdfsUtil.getFileSystem();
		// 递归删除
		boolean deleted = fs.delete(new Path("/beifeng/api/1.txt"), true);
		System.out.println(deleted);
		// 删除空文件夹，不递归
		deleted = fs.delete(new Path("/beifeng/api/mkdirs"), false);
		System.out.println(deleted);
		// 删除 非空文件夹，不递归
		deleted = fs.delete(new Path("/beifeng/api"), false);
		System.out.println(deleted);
		fs.close();
	}

	@Test
	public void testDeleteOnExit() throws IOException {
		FileSystem fs = HdfsUtil.getFileSystem();
		fs.deleteOnExit(new Path("/beifeng/api/createNewFile1.txt"));
		System.in.read();
		fs.close();
	}

	@Test
	public void testGetFileStatus() throws IOException {
		FileSystem fs = HdfsUtil.getFileSystem();
		FileStatus status = fs.getFileStatus(new Path("/beifeng/api/2.txt"));
		System.out.println(status.isDirectory() ? "文件夹" : "文件");
		System.out.println("提交时间:" + status.getAccessTime());
		System.out.println("复制因子:" + status.getReplication());
		System.out.println("长度：" + status.getLen());
		System.out.println("最后修改时间：" + status.getModificationTime());
		System.out.println("块大小：" + status.getBlockSize());
	}

}
