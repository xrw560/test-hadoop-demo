package com.nuct.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ncut.hdfs.utils.HdfsUtil;

public class WordCountRunner implements Tool {
	private Configuration conf = null;

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration configuration) {
		this.conf = configuration;
		this.conf.set("fs.defaultFs", "hdfs://192.168.138.50");
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wordcount");
		// 五个阶段

		// 1.输入
		FileInputFormat.setInputPaths(job, new Path("/beifeng/input"));

		// 2. map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 3. shuffle

		// 4. reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 5.output
		HdfsUtil.deleteFile("/beifeng/output/wordcount/1");
		FileOutputFormat.setOutputPath(job, new Path("/beifeng/output/wordcount/1"));

		// 成功 返回0，失败返回1
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// 运行
		ToolRunner.run(new WordCountRunner(), args);
	}

}
