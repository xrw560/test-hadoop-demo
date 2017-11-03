package com.ncut.mr.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuffer sb = null;
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (Text value : values) {
			sb = new StringBuffer();

			String line = value.toString();
			// StringBuffer sb = new StringBuffer(line);
			// 反转，避免文档路径中包含":"
			// sb.reverse().toString().split(":",2);

			// 反转
			line = sb.append(line).reverse().toString();
			// strs[0]:出现次数，strs[1]:文档路径
			String[] strs = line.split(":", 2);
			sb.delete(0, sb.length() - 1);
			// 将路径地址反转，恢复原样
			String path = sb.append(strs[1]).reverse().toString();
			int count = Integer.valueOf(strs[0]);
			if (map.containsKey(path)) {
				map.put(path, map.get(path) + count);
			} else {
				map.put(path, count);
			}
		}

		sb = new StringBuffer();
		for (Entry<String, Integer> entry : map.entrySet()) {
			// 文档之间以";"分隔
			sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
		}
		// 去除最后一个";"
		outputValue.set(sb.deleteCharAt(sb.length() - 1).toString());
		context.write(key, outputValue);
	}

}
