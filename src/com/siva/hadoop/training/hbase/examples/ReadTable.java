package com.siva.hadoop.training.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadTable {
	public static String tableName = "wordcount_output";
	public static String columnFamily = "cf";
	public static String qualifier = "count";

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);

		TableName tblName = TableName.valueOf(tableName);
		Table table = connection.getTable(tblName);

		// scan operation
		Scan s = new Scan();
		ResultScanner scanner = table.getScanner(s);
		try {
			for (Result row : scanner) {
				byte[] key = row.getRow();
				String rowKey = Bytes.toString(key);

				byte[] val = row.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
				String value = Bytes.toString(val);

				System.out.println("key: " + rowKey + ",  value: " + value);
			}
		} finally {
			scanner.close();
		}
	}
}
