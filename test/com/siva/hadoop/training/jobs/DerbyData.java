package com.siva.hadoop.training.jobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DerbyData {
	private static String driverName = "org.apache.derby.jdbc.EmbeddedDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:derby:;databaseName=/home/hadoop/work/warehouse/metastore_db", "", "");
		Statement stmt = con.createStatement();

		// show databases
		String query = "select * from DBS";
		System.out.println("Running: " + query);
		ResultSet res = stmt.executeQuery(query);
		if (res.next()) {
			System.out.println(res.getString(1));
			System.out.println(res.getString(2));
			System.out.println(res.getString(3));
		}

	}
}
