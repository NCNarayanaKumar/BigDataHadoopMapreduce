package com.siva.hadoop.training.sqoop;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.tool.ImportTool;

public class SqoopExamples {

	private static SqoopOptions SqoopOptions = new SqoopOptions();
	private static final String connectionString = "jdbc:mysql://127.0.0.1:3306/sqoop";
	private static final String username = "root";
	private static final String password = "hadoop";

	private static void setUp() {
		SqoopOptions.setConnectString(connectionString);
		SqoopOptions.setUsername(username);
		SqoopOptions.setPassword(password);
	}

	private static int runIt() {
		int res;
		res = new ImportTool().run(SqoopOptions);
		if (res != 0) {
			throw new RuntimeException("Sqoop API Failed - return code : " + Integer.toString(res));
		}
		return res;
	}

	private static void TransferringEntireTable(String table) {
		SqoopOptions.setTableName(table);
	}

	private static void TansferringEntireTableSpecificDir(String table, String directory) {
		TransferringEntireTable(table);
		SqoopOptions.setWarehouseDir(directory);
	}

	private static void TansferringEntireTableSpecificDirHiveMerge(String table, String directory) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setHiveImport(true);
	}

	private static void TansferringEntireTableSpecificDirHivePartitionMerge(String table, String directory, String partitionKey,
			String partitionValue) {
		TansferringEntireTableSpecificDirHiveMerge(table, directory);
		SqoopOptions.setHivePartitionKey(partitionKey);
		SqoopOptions.setHivePartitionValue(partitionValue);
	}

	private static void TansferringEntireTableWhereClause(String table, String whereClause) {
		// To do
	}

	private static void CompressingImportedData(String table, String directory, String compress) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setCompressionCodec(compress);
	}

	private static void incrementalImport(String table, String directory, IncrementalMode mode, String checkColumn, String lastVale) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setIncrementalMode(mode);
		SqoopOptions.setAppendMode(true);
		SqoopOptions.setIncrementalTestColumn(checkColumn);
		SqoopOptions.setIncrementalLastValue(lastVale);
	}

	private static void TransferringEntireTableSpecificDirHive(String table, String directory) {
		TansferringEntireTableSpecificDir(table, directory);
		SqoopOptions.setHiveImport(true);
	}

	private static void TransferringEntireTableSpecificDirHivePartition(String table, String directory, String partitionKey,
			String partitionValue) {
		TransferringEntireTableSpecificDirHive(table, directory);
		SqoopOptions.setHivePartitionKey(partitionKey);
		SqoopOptions.setHivePartitionValue(partitionValue);
	}
}