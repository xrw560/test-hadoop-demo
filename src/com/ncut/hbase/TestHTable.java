package com.ncut.hbase;

import java.awt.print.Printable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.tools.internal.jxc.gen.config.Config;

public class TestHTable {

	Configuration conf;
	// HTable htable;
	HTableInterface htable;
	ExecutorService pool;
	HConnection connection;
	byte[] family = Bytes.toBytes("f");

	@Before
	public void setUp() throws Exception {
		conf = HBaseUtil.getHBaseConfiguration();
		// htable = new HTable(conf, "users");
		htable = getHTable(conf, "users");
	}

	public HTableInterface getHTable(Configuration conf, String tableName) throws IOException {
		pool = Executors.newFixedThreadPool(10);
		connection = HConnectionManager.createConnection(conf, pool);
		HTableInterface htable = connection.getTable(Bytes.toBytes(tableName));
		return htable;
	}

	@After
	public void tearDown() throws Exception {
		if (htable != null) {
			htable.close();
		}
		if (connection != null) {
			connection.close();
		}
		if (pool != null && !pool.isShutdown()) {
			pool.shutdown();
			pool.awaitTermination(0, TimeUnit.SECONDS);
			System.out.println("关闭pool");
		}

	}

	@Test
	public void testSinglePut() throws Exception {
		Put put = new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("1"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes(23));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("phone"), Bytes.toBytes("010-11111111"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("email"), Bytes.toBytes("zhangsan@qq.ccom"));
		htable.put(put);

	}

	@Test
	public void testMultiPuts() throws Exception {

		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("2"));
		put2.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user2"));

		Put put3 = new Put(Bytes.toBytes("row3"));
		put3.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("3"));
		put3.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user3"));

		Put put4 = new Put(Bytes.toBytes("row4"));
		put4.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("4"));
		put4.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user4"));

		List puts = new ArrayList();
		puts.add(put2);
		puts.add(put3);
		puts.add(put4);
		htable.put(puts);
	}

	@Test
	public void testCheckAndPut() throws IOException {

		// 检测put,条件成功就插入，要求key是一样的
		Put put = new Put(Bytes.toBytes("row5"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("6"));
		htable.checkAndPut(Bytes.toBytes("row5"), Bytes.toBytes("f"), Bytes.toBytes("id"), null, put);

	}

	@Test
	public void testGet() throws IOException {
		Get get = new Get(Bytes.toBytes("row1"));
		Result result = htable.get(get);
		byte[] buf = result.getValue(family, Bytes.toBytes("id"));
		System.out.println("id:" + Bytes.toString(buf));
		buf = result.getValue(family, Bytes.toBytes("name"));
		System.out.println("name:" + Bytes.toString(buf));
		buf = result.getValue(family, Bytes.toBytes("age"));
		System.out.println("age:" + Bytes.toInt(buf));

		buf = result.getRow();
		System.out.println("row:" + Bytes.toString(buf));

	}

	@Test
	public void testDelete() throws IOException {

		Delete delete = new Delete(Bytes.toBytes("row4"));
		// 直接删除family
		// delete.deleteFamily(family);

		// 删除列
		delete.deleteColumn(family, Bytes.toBytes("id"));
		delete.deleteColumn(family, Bytes.toBytes("name"));

		htable.delete(delete);

	}

	@Test
	public void testScan() throws Exception {
		Scan scan = new Scan();
		// scan.addColumn(family, Bytes.toBytes("id"));
		scan.setStartRow(Bytes.toBytes("row1"));
		scan.setStopRow(Bytes.toBytes("row5"));
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		byte[][] prefixs = new byte[2][];
		prefixs[0] = Bytes.toBytes("id");
		prefixs[1] = Bytes.toBytes("name");
		MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixs);
		filterList.addFilter(multipleColumnPrefixFilter);
		scan.setFilter(filterList);

		ResultScanner scanner = htable.getScanner(scan);
		// Iterator<Result> it = scanner.iterator();
		// while (it.hasNext()) {
		// Result result = it.next();
		// print(result);
		// }
		for (Result result : scanner) {
			print(result);
		}
	}

	private void print(Result result) {
		System.out.println("*********************************" + Bytes.toString(result.getRow()));
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
		for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
			String family = Bytes.toString(entry.getKey());
			for (Entry<byte[], NavigableMap<Long, byte[]>> columnFamily : entry.getValue().entrySet()) {
				String column = Bytes.toString(columnFamily.getKey());
				String value = "";
				if ("age".equals(column)) {
					value = "" + Bytes.toInt(columnFamily.getValue().firstEntry().getValue());
				} else {
					value = Bytes.toString(columnFamily.getValue().firstEntry().getValue());
				}
				System.out.println(family + ":" + column + ":" + value);
			}
		}
	}

}
