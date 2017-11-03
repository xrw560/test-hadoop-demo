package com.nuct.mr.shuffle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 主类
 *
 */
public class DemoRunner {

	/**
	 * 处理mapper类
	 * 
	 */
	static class DemoMapper extends Mapper<Object, Text, IntPair, IntWritable> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split("\\s");
			if (strs.length == 2) {
				int first = Integer.valueOf(strs[0]);
				int second = Integer.valueOf(strs[1]);
				context.write(new IntPair(first, second), new IntWritable(second));
			} else {
				System.out.println("数据异常" + line);
			}
		}
	}

	/**
	 * 自定义reducer
	 * 
	 * 使用自定义grouping,reducer的输入为(31,32)或者(31,33)等都在同一个key分组中，并且是排序好的
	 *
	 */
	static class DemoReducer extends Reducer<IntPair, IntWritable, IntWritable, Text> {
		@Override
		protected void reduce(IntPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Integer preKey = key.getFirst();
			StringBuffer sb = new StringBuffer();
			for (IntWritable value : values) {
				int curKey = key.getFirst();
				// if (preKey == curKey) {
				// 表示同一个key,但是value是不一样的或者是value是排序好的
				sb.append(value.get()).append(",");
				// }
				// else {
				// System.out.println("----------------->key:" + key + "
				// preKey:" + preKey);
				// // 表示是新的一个key,先输出旧的key对应的value信息，然后修改key值和StringBuffer的值
				// context.write(new IntWritable(preKey), new
				// Text(sb.toString()));
				// preKey = curKey;
				// sb = new StringBuffer();
				// sb.append(value.get()).append(",");
				// }
			}

			// 输出最后的结果信息
			context.write(new IntWritable(preKey), new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.138.50");
		Job job = Job.getInstance(conf, "demo-job");

		job.setJarByClass(DemoRunner.class);
		job.setMapperClass(DemoMapper.class);
		job.setReducerClass(DemoReducer.class);

		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// group by class
		job.setGroupingComparatorClass(IntPairGrouping.class);
		// 设置partitioner,要求reducer个数必须大于1
		job.setPartitionerClass(IntPairPartitioner.class);
		job.setNumReduceTasks(2);

		// 输入输出路径
		FileInputFormat.setInputPaths(job, new Path("/beifeng/input"));
		FileOutputFormat.setOutputPath(job, new Path("/beifeng/output" + System.currentTimeMillis()));

		job.waitForCompletion(true);
	}
}
