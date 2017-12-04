package com.siva.hadoop.training.jobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HadoopHiveUDFTest {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	private static String tableName = "UDFTest";

	private static String partitionedTableName = "pa";

	private String dataFilePath = "/home/hadoop/work/testdata/k1.log";

	private String dataFilePath1 = "/home/hadoop/work/testdata/table1.txt";

	private String dataFilePath2 = "/home/hadoop/work/testdata/table2.txt";

	private boolean standAloneServer = true;

	public static String hiveURI = "jdbc:hive://localhost:10000/default";

	// public static String hiveURI =
	// "jdbc:hive://10.253.116.170:10000/default";

	public HadoopHiveUDFTest(String name) {
		standAloneServer = true;
	}

	protected Connection getConnection() throws Exception {
		Connection con = null;

		Class.forName(driverName);
		if (standAloneServer) {
			con = DriverManager.getConnection(hiveURI, "", "");
		} else {
			con = DriverManager.getConnection("jdbc:hive://", "", "");
		}

		return con;
	}

	protected Statement setUpSimpleTable(Connection con) throws Exception {
		Statement stmt = null;
		stmt = con.createStatement();
		// drop table. ignore error.

		// stmt.executeQuery("drop table " + tableName);

		// create table
		ResultSet res = stmt.executeQuery("create table " + tableName
				+ " (key double, value string)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");

		// load data

		res = stmt.executeQuery("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
		res = stmt.executeQuery("show tables");
		while (res.next()) {
			System.out.println(res.getString(1));
		}
		return stmt;

		/*
		 * stmt.executeQuery("drop table xtable");
		 * stmt.executeQuery("drop table ytable");
		 * 
		 * ResultSet res = stmt.executeQuery(
		 * "create table xtable(id int,name string)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
		 * );
		 * 
		 * res = stmt.executeQuery(
		 * "create table ytable(name string,country string)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
		 * );
		 * 
		 * 
		 * res = stmt.executeQuery("load data local inpath '" +
		 * dataFilePath1.toString() + "' into table  xtable");
		 * 
		 * res = stmt.executeQuery("load data local inpath '" +
		 * dataFilePath2.toString() + "' into table ytable");
		 * 
		 * return stmt;
		 */
	}

	protected void addUDFJar(Statement stmt) throws Exception {
		String addJarquery = "add jar /home/hadoop/work/test/jars/aMovingAverage.jar";
		ResultSet res = stmt.executeQuery(addJarquery);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
		String createTempFun = "create temporary function mavg as 'orienit.hadoop.training.hive.udf.examples.UDFMovingAverage'";
		res = stmt.executeQuery(createTempFun);
	}

	protected Statement setUpPartitionTable(Connection con) throws Exception {
		Statement stmt = null;

		stmt = con.createStatement();

		ResultSet res = stmt.executeQuery("drop table " + partitionedTableName);
		res = stmt.executeQuery("create table " + partitionedTableName
				+ " (key double, value string) partitioned by (dt STRING)");
		res = stmt.executeQuery("load data local inpath '" + dataFilePath.toString() + "' into table "
				+ partitionedTableName + " PARTITION (dt='20090619')");
		return stmt;
	}

	protected ResultSet executeQuery(Statement stmt, String query) throws Exception {
		ResultSet res = stmt.executeQuery(query);
		return res;
	}

	protected static void printResultSet(ResultSet res) throws Exception {
		if (res == null)
			return;
		while (res.next()) {
			System.out.println(res.getDouble(1) + "," + res.getDouble(2) + "," + res.getString(3));
		}
	}

	public static void main(String[] args) {
		try {
			System.out.println("Starting Test...");
			HadoopHiveUDFTest test = new HadoopHiveUDFTest("Hive");
			Thread.sleep(1000);
			System.out.println("Connecting to Hive....");
			Connection con = test.getConnection();
			System.out.println("Setting up sample table....");
			Statement stmt = test.setUpSimpleTable(con);

			ResultSet set = test.executeQuery(stmt, "select * from UDFTest");
			while (set.next()) {
				System.out.println(set.getString(1));
			}

			set = test.executeQuery(stmt, "select UDFTest.value from UDFTest where UDFTest.value = " + 100.0);
			while (set.next()) {
				System.out.println(set.getString(1));
			}

			set = test.executeQuery(stmt, "select count(value) from UDFTest");
			while (set.next()) {
				System.out.println(set.getString(1));
			}

			set = test.executeQuery(stmt, "select sum(1) from UDFTest");
			while (set.next()) {
				System.out.println(set.getString(1));
			}

			// JOIN Query....
			/*
			 * Statement stmt = con.createStatement(); ResultSet res = test
			 * .executeQuery(stmt,
			 * "select * from xtable t1 join ytable t2 on t1.name=t2.name");
			 * 
			 * while(res.next()) { System.out.print(res.getString(1) +", ");
			 * System.out.print(res.getString(2) +", ");
			 * System.out.println(res.getString(3)); }
			 */

			// UDF related......
			/*
			 * Statement stmt = con.createStatement(); set =
			 * test.executeQuery(stmt, "select COUNT(1) from UDFTest");
			 * System.out.println(" COUNT Result..........."); while
			 * (set.next()) { System.out.println(set.getString(1)); } System.out
			 * .println(" Adding the UDF Jar and creating temp fun'n.....");
			 * test.addUDFJar(stmt);
			 * System.out.println("Executing sammple query...."); set =
			 * test.executeQuery(stmt,
			 * "select mavg(key, 3.1),key,value from UDFTest");
			 * System.out.println("Printing results..."); printResultSet(set);
			 */
			// con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}