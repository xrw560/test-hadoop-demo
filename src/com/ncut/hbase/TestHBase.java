package com.ncut.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBase {

	Configuration conf;
	HBaseAdmin hbaseAdmin;

	@Before
	public void setUp() throws Exception {
		conf = HBaseUtil.getHBaseConfiguration();
		hbaseAdmin = new HBaseAdmin(conf);
	}

	@After
	public void tearDown() throws Exception {
		if (hbaseAdmin != null) {
			hbaseAdmin.close();
		}
	}

	/**
	 * 创建table
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCreateTable() throws Exception {
		TableName tableName = TableName.valueOf("users");
		if (hbaseAdmin.tableExists(tableName)) {//判断表 是否存在
			System.out.println("表已存在");
			return;
		}
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor("f"));
		desc.setMaxFileSize(10000L);
		hbaseAdmin.createTable(desc);
		System.out.println("创建表成功");
	}

	/**
	 * 获取表信息
	 * 
	 * @throws Exception
	 */
	@Test
	public void testGetTableDescriptor() throws Exception {
		TableName tableName = TableName.valueOf("users");
		if (!hbaseAdmin.tableExists(tableName)) {//判断表 是否存在
			System.out.println("表不存在");
			return;
		}
		HTableDescriptor desc = hbaseAdmin.getTableDescriptor(tableName);
		System.out.println(desc);
	}

	/**
	 * 删除表
	 * 
	 * @throws IOException
	 */
	@Test
	public void testDeleteTable() throws IOException {
		TableName tableName = TableName.valueOf("users");
		if (hbaseAdmin.tableExists(tableName)) {//判断表 是否存在
			if (hbaseAdmin.isTableEnabled(tableName)) {//判断表的状态是Enabled还是disabled
				hbaseAdmin.disableTable(tableName);
			}
			hbaseAdmin.deleteTable(tableName);
			System.out.println("删除表成功");
		} else {
			System.out.println("表不存在");
		}
	}

}
