package com.siva.hadoop.training.sqoop;

import com.cloudera.sqoop.Sqoop;

public class SqoopTest2 {

	public static void main(String[] args) {
		String[] ret = { "import", "--connect", "jdbc:mysql://localhost:3306/sqoop", "--username", "root", "--password", "hadoop",
				"--table", "pet", "--target-dir", "hdfs://localhost:9000/user/sqoop/mysqoop", "--split-by", "name",
				"--fields-terminated-by", "'\t'", "--lines-terminated-by", "'\n'" };
		Sqoop.runTool(ret);
	}
}
