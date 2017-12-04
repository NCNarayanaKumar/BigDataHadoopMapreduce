package com.siva.hadoop.training.jobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleData {
	private static String driverName = "oracle.jdbc.OracleDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:oracle:thin:@myhost:1521:databaseName", "", "");
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
