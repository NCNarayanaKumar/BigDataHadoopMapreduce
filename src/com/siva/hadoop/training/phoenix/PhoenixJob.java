package com.siva.hadoop.training.phoenix;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixJob {

	public static void main(String[] args) throws SQLException, ClassNotFoundException, FileNotFoundException {
		Statement stmt = null;
		ResultSet rset = null;

		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
		stmt = con.createStatement();
		con.setAutoCommit(false);
		// con.setNetworkTimeout(Executors..newFixedThreadPool(1), 1024 * 1024 *
		// 12);
		// .getNetworkTimeout()....setWriteBufferSize(1024 * 1024 * 12);
		// java -Xloggc:gc.log GarbageCollector in cmd
		try {
			BufferedReader br = new BufferedReader(new FileReader(args[0]));
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {

				String rec = sCurrentLine.toString();
				String[] rec2 = rec.split("\t");

				// ResultSet rset = null;
				boolean flag = false;

				try {
					String myquery = "select * from \"HISTORICAL_DATA_NEW\" where \"INVOICE_NUMBER\" = '" + rec2[11] + "'";
					// System.out.println(myquery);
					PreparedStatement statement = con.prepareStatement(myquery);
					rset = statement.executeQuery();
					while (rset.next()) {
						flag = true;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				if (!flag) {
					try {
						// context.write(new
						// ImmutableBytesWritable(rec2[10].getBytes()),put);
						String insQue = "upsert into \"HISTORICAL_DATA_NEW\" values ('" + rec2[11] + "','" + rec2[1] + "','" + rec2[2]
								+ "','" + rec2[3] + "','" + rec2[4] + "','" + rec2[5] + "','" + rec2[6] + "','" + rec2[7] + "','" + rec2[8]
								+ "','" + rec2[9] + "','" + rec2[11] + "','" + rec2[12] + "','" + rec2[13] + "')";
						stmt.executeUpdate(insQue);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					try {
						// stmt2.executeUpdate("create table if not exists DUPLICATES (Invoice_Number varchar not null primary key, Invoice_Date varchar,  Invoice_Amount varchar)");
						String delQue = "upsert into \"DUPLICATES\" values ('" + rec2[11] + "','" + rec2[12] + "','" + rec2[13] + "')";
						stmt.executeUpdate(delQue);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			con.commit();
			stmt.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
