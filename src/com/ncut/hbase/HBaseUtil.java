package com.ncut.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseUtil {

	/**
	 * 获取HBase的配置文件信息
	 * 
	 * @return
	 */
	public static Configuration getHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.138.50");
		return conf;
	}

}
