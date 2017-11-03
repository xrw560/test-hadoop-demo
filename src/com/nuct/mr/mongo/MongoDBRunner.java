package com.nuct.mr.mongo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * 操作mongodb的主类
 * 
 * @author Administrator
 *
 */
public class MongoDBRunner {

	/**
	 * mongodb转换到hadoop的一个bean对象
	 * 
	 * @author Administrator
	 *
	 */
	static class PersonMongoDBWritable implements MongoDBWritable {

		private String name;
		private Integer age;
		private String sex = "";
		private int count = 1;

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.name);
			out.writeUTF(this.sex);
			if (this.age == null) {
				out.writeBoolean(false);
			} else {
				out.writeBoolean(true);
				out.writeInt(this.age);
			}
			out.writeInt(this.count);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.name = in.readUTF();
			this.sex = in.readUTF();
			if (in.readBoolean()) {
				this.age = in.readInt();
			} else {
				// 把前一个覆盖掉
				this.age = null;
			}
			this.count = in.readInt();
		}

		@Override
		public void readFields(DBObject dbObject) {

			this.name = dbObject.get("name").toString();
			if (dbObject.get("age") != null) {
				// 17.0只取整数部分
				this.age = Double.valueOf(dbObject.get("age").toString()).intValue();
			} else {
				this.age = null;
			}
		}

		@Override
		public void write(DBCollection dbCollection) {
			DBObject dbObject = BasicDBObjectBuilder.start().add("age", this.age).add("count", this.count).get();
			dbCollection.insert(dbObject);
		}

	}

	/**
	 * mapper
	 *
	 */
	static class MongoDBMapper extends Mapper<LongWritable, PersonMongoDBWritable, IntWritable, PersonMongoDBWritable> {
		@Override
		protected void map(LongWritable key, PersonMongoDBWritable value, Context context)
				throws IOException, InterruptedException {
			if (value.age == null) {
				System.out.println("过滤数据 " + value.name);
				return;
			}
			context.write(new IntWritable(value.age), value);
		}

	}

	/**
	 * 自定义Reducer
	 * 
	 */
	static class MongoDBReducer
			extends Reducer<IntWritable, PersonMongoDBWritable, NullWritable, PersonMongoDBWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<PersonMongoDBWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (PersonMongoDBWritable value : values) {
				sum += value.count;
			}
			PersonMongoDBWritable personMongoDBWritable = new PersonMongoDBWritable();
			personMongoDBWritable.age = key.get();
			personMongoDBWritable.count = sum;
			context.write(NullWritable.get(), personMongoDBWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// 设置inputformat的value类
		conf.setClass("mapreduce.mongo.split.value.class", PersonMongoDBWritable.class, MongoDBWritable.class);
		Job job = Job.getInstance(conf, "自定义input/outformat");
		job.setJarByClass(MongoDBRunner.class);
		job.setMapperClass(MongoDBMapper.class);
		job.setReducerClass(MongoDBReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);// mapper输出key
		job.setMapOutputValueClass(PersonMongoDBWritable.class);// mapper输出value
		job.setOutputKeyClass(NullWritable.class);// reducer输出key
		job.setOutputValueClass(PersonMongoDBWritable.class);// reducer输出value
		job.setInputFormatClass(MongoDBInputFormat.class);// 设置inputformat
		job.setOutputFormatClass(MongoDBOutputFormat.class);// 设置outputformat
		job.waitForCompletion(true);
	}

}
