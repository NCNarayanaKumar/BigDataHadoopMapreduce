package com.siva.hadoop.training.hbase.examples;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class BasicCommands {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		try {
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf("mytable");
			boolean tableExists = admin.tableExists(tableName);
			if (tableExists) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}

			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

			HColumnDescriptor columnDescriptor1 = new HColumnDescriptor("cf1");
			columnDescriptor1.setValue("VERSIONS", "4");
			columnDescriptor1.setValue("TTL", "10");

			HColumnDescriptor columnDescriptor2 = new HColumnDescriptor("cf2");

			tableDescriptor.addFamily(columnDescriptor1);
			tableDescriptor.addFamily(columnDescriptor2);

			admin.createTable(tableDescriptor);

			Table table = connection.getTable(tableName);
			try {
				byte[] row1 = Bytes.toBytes("row1");
				byte[] row2 = Bytes.toBytes("row2");

				byte[] cf1 = Bytes.toBytes("cf1");
				byte[] cf2 = Bytes.toBytes("cf2");

				byte[] q1 = Bytes.toBytes("a");
				byte[] value1 = Bytes.toBytes("value1");

				byte[] q2 = Bytes.toBytes("b");
				byte[] value2 = Bytes.toBytes("value2");

				// put operation
				Put p = new Put(row1);
				p.add(cf1, q1, value1);
				p.add(cf1, q2, value2);
				table.put(p);

				p = new Put(row2);
				p.add(cf1, q1, value1);
				p.add(cf1, q2, value2);
				table.put(p);

				// get operation
				Get g = new Get(row1);
				Result r = table.get(g);
				byte[] cell = r.getValue(cf1, q1);

				String valueStr = Bytes.toString(cell);
				System.out.println("GET: " + valueStr);

				List<KeyValue> keyValues = r.getColumn(cf1, q1);
				for (KeyValue keyValue : keyValues) {
					System.out.println("KeyValue: " + keyValue);
				}

				// scan operation
				Scan s = new Scan();
				s.addColumn(cf1, q1);
				ResultScanner scanner = table.getScanner(s);
				try {
					for (Result rr : scanner) {
						System.out.println("Found row: " + rr);
					}
				} finally {
					scanner.close();
				}
			} finally {
				if (table != null)
					table.close();
			}
		} finally {
			connection.close();
		}
	}
}
