package com.ncut.mr.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ncut.hdfs.utils.HdfsUtil;

public class ReverseIndexRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HdfsUtil.getConfiguration();

		Job job = Job.getInstance(conf, "reverse_index");

		job.setJarByClass(ReverseIndexRunner.class);

		FileInputFormat.setInputPaths(job, new Path("/beifeng/input"));
		job.setMapperClass(ReverseIndexMapper.class);
		job.setReducerClass(ReverseIndexReducer.class);
		// mapper与reducer的输入输出一致，可以不设置mapper.outputKeyClass&valueClass
		// mapper默认会去job.keyclass与valueclass中取
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path("/beifeng/index/" + System.currentTimeMillis()));
		job.waitForCompletion(true);

	}

}
