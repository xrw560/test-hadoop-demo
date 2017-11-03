package com.ncut.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * mapreduce操作hbase
 * 
 */
public class HBaseTableDemo {

	/**
	 * 转换字符串为map对象,字符串： '{"p_id":"100009","p_name":"<厦门双飞4日游>","price":"1388"}'
	 * 
	 * @param content
	 * @return
	 */
	static Map<String, String> transformContent2Map(String content) {
		Map<String, String> map = new HashMap<String, String>();
		int i = 0;
		String key = "";
		StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
		while (tokenizer.hasMoreTokens()) {
			if (++i % 2 == 0) {
				// 当前值为value
				map.put(key, tokenizer.nextToken());
			} else {
				// 当前的值为key
				key = tokenizer.nextToken();
			}
		}
		return map;
	}

	/**
	 * mapper类，从hbase输入数据
	 *
	 */
	static class DemoMapper extends TableMapper<Text, ProductModel> {
		private Text outputKey = new Text();
		private ProductModel outputValue = new ProductModel();

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
			if (content == null) {
				System.out.println("数据格式错误" + content);
				return;
			}
			Map<String, String> map = HBaseTableDemo.transformContent2Map(content);
			if (map.containsKey("p_id")) {
				// 产品id存在
				outputKey.set(map.get("p_id"));
			} else {
				System.out.println("数据格式错误" + content);
				return;
			}
			if (map.containsKey("p_name") && map.containsKey("price")) {

				// 数据正常，进行赋值
				outputValue.setId(map.get("p_id"));
				outputValue.setName(map.get("p_name"));
				outputValue.setPrice(map.get("price"));
			} else {
				System.out.println("数据格式错误" + content);
				return;
			}
			context.write(outputKey, outputValue);
		}
	}

	/**
	 * mapper类，从hbase输入数据，不使用reducer，直接输出到hbase
	 * 
	 */
	static class DemoMapper2 extends TableMapper<ImmutableBytesWritable, Put> {
		private Text outputKey = new Text();
		private ProductModel outputValue = new ProductModel();

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
			if (content == null) {
				System.out.println("数据格式错误" + content);
				return;
			}
			Map<String, String> map = HBaseTableDemo.transformContent2Map(content);
			ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
			if (map.containsKey("p_id")) {
				// 产品id存在
				outputKey = new ImmutableBytesWritable(Bytes.toBytes(map.get("p_id")));
			} else {
				System.out.println("数据格式错误" + content);
				return;
			}
			Put put = new Put(Bytes.toBytes(map.get("p_id")));
			if (map.containsKey("p_name") && map.containsKey("price")) {

				// 数据正常，进行赋值
				put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(map.get("p_id")));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(map.get("p_name")));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(map.get("price")));
			} else {
				System.out.println("数据格式错误" + content);
				return;
			}
			context.write(outputKey, put);
		}
	}

	/**
	 * 向Hbase输出reducer
	 * 
	 */
	static class DemoReducer extends TableReducer<Text, ProductModel, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text key, Iterable<ProductModel> values, Context context)
				throws IOException, InterruptedException {
			for (ProductModel value : values) {
				// 如果有多个产品id的话，只拿一个
				ImmutableBytesWritable outputKey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
				Put put = new Put(Bytes.toBytes(key.toString()));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(value.getPrice()));
				context.write(outputKey, put);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// 本地选择：initLocalHBaseMapReducerJobConfig1或initLocalHBaseMapReducerJobConfig1
		// 集群选择：initLocalHBaseMapReducerJobConfig2
		Job job = initLocalHBaseMapReducerJobConfig3();
		int l = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("执行：" + l);
	}

	/**
	 * 仅支持本地运行
	 * 
	 * @return
	 * @throws Exception
	 */
	static Job initLocalHBaseMapReducerJobConfig1() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.138.50");// hadoop环境
		conf.set("hbase.zookeeper.quorum", "192.168.138.50");// hbase zk环境信息

		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(HBaseTableDemo.class);
		// 设置mapper相关，mapper从hbase输入
		// 本地环境，而且fs.defaultFS为集群模式的时候，需要设置addDependencyJars参数为false
		TableMapReduceUtil.initTableMapperJob("data", new Scan(), DemoMapper.class, Text.class, ProductModel.class, job,
				false);

		// 设置reducer相关,reducer向hbase输出
		// 本地环境，而且fs.defaultFS为集群模式的时候，需要设置addDependencyJars参数为false
		TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class, job, null, null, null, null, false);
		return job;
	}

	/**
	 * 本地或集群都可以选择
	 * 
	 * @return
	 * @throws Exception
	 */
	static Job initLocalHBaseMapReducerJobConfig2() throws Exception {
		Configuration conf = new Configuration();
		// 不要hadoop的配置信息
		// conf.set("fs.defaultFS", "hdfs://192.168.138.50");// hadoop环境
		conf.set("hbase.zookeeper.quorum", "192.168.138.50");// hbase zk环境信息

		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(HBaseTableDemo.class);
		// 设置mapper相关，mapper从hbase输入
		// 本地环境，而且fs.defaultFS为集群模式的时候，需要设置addDependencyJars参数为false
		// 集群环境中，addDependencyJars必须为true
		TableMapReduceUtil.initTableMapperJob("data", new Scan(), DemoMapper.class, Text.class, ProductModel.class, job,
				true);

		// 设置reducer相关,reducer向hbase输出
		// 本地环境，而且fs.defaultFS为集群模式的时候，需要设置addDependencyJars参数为false
		// 集群环境中，addDependencyJars必须为true
		TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class, job, null, null, null, null, true);
		return job;
	}

	/**
	 * 直接使用mapper进行hbase输出，不使用reducer进行输出
	 * 
	 * @return
	 * @throws Exception
	 */
	static Job initLocalHBaseMapReducerJobConfig3() throws Exception {
		Configuration conf = new Configuration();
		// 不要hadoop的配置信息
		conf.set("fs.defaultFS", "hdfs://192.168.138.50");// hadoop环境
		conf.set("hbase.zookeeper.quorum", "192.168.138.50");// hbase zk环境信息

		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(HBaseTableDemo.class);
		// 设置mapper相关，mapper从hbase输入
		// 本地环境，而且fs.defaultFS为集群模式的时候，需要设置addDependencyJars参数为false
		// 集群环境中，addDependencyJars必须为true
		TableMapReduceUtil.initTableMapperJob("data", new Scan(), DemoMapper2.class, ImmutableBytesWritable.class,
				Put.class, job, false);

		// 设置reducer相关,reducer向hbase输出
		// 本地环境，而且fs.defaultFS为集群模式的时候，需要设置addDependencyJars参数为false
		// 集群环境中，addDependencyJars必须为true
		TableMapReduceUtil.initTableReducerJob("online_product", null, job, null, null, null, null, false);
		job.setNumReduceTasks(0);
		return job;
	}

}
