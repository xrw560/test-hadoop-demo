package com.nuct.mr.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, LongWritable> {

	private Text word = new Text();
	private LongWritable one = new LongWritable(1);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		System.out.println("调用 WordCountMapper的setup方法");
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("调用WordCountMapper的map方法");
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, one);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		System.out.println("调用WordCountMapper的cleanup方法");
	}
}
