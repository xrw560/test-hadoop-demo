package com.nuct.mr.mongo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * mongodb自定义数据类型
 * 
 *
 */
public interface MongoDBWritable extends Writable {
	/**
	 * 从Mongodb中读取数据
	 * 
	 * @param dbObject
	 */
	public void readFields(DBObject dbObject);

	/**
	 * 往mongodb中写入数据
	 * 
	 * @param dbCollection
	 */
	public void write(DBCollection dbCollection);
	

}
