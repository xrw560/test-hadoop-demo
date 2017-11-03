package com.nuct.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable count = new LongWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		System.out.println("调用WordCountReducer的setup方法");
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("调用WordCountReducer的reduce方法");
		long sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		count.set(sum);
		context.write(key, count);

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		System.out.println("调用WordCountReducer的cleanup方法");
	}

}
