package com.siva.hadoop.training.sqoop;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ImportTool;

public class SqoopImport {
	public static void main(String[] args) {
		SqoopOptions options = new SqoopOptions();
		options.setConnectString("jdbc:mysql://localhost:3306/sqoop");
		options.setTableName("pet");
		options.setColumns(new String[] { "name", "owner" });

		// this where clause works when importing whole table,
		// i.e when setTableName() is used
		options.setWhereClause("sex='f'");
		options.setUsername("root");
		options.setPassword("hadoop");
		options.setSplitByCol("name");

		// Make sure the direct mode is off when importing data to HBase
		// options.setDirectMode(true);

		options.setNumMappers(8); // Default value is 4
		options.setSqlQuery("SELECT * FROM pet");
		options.setTargetDir("hdfs://localhost:9000/user/hadoop/pet1111");
		int ret = new ImportTool().run(options);
	}
}
