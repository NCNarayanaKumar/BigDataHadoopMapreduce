package com.siva.hadoop.training.sqoop;

import com.cloudera.sqoop.Sqoop;

public class SqoopDriver {
	public static void main(String[] args) {
		String[] str = { "import", "--connect", "jdbc:mysql://localhost:3306/sqoop", "--username", "root", "--password", "hadoop",
				"--table", "pet", "--target-dir", "hdfs://localhost:9000/user/sqoop/pet", "--split-by", "name" };

		Sqoop.runTool(str);
	}
}
