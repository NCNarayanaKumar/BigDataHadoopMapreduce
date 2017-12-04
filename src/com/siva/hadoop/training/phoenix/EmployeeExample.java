package com.siva.hadoop.training.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class EmployeeExample {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Statement stmt = null;
		ResultSet rset = null;

		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
		stmt = con.createStatement();

		stmt.executeUpdate("create table if not exists employee (id integer not null primary key, name varchar, designation varchar, salary double)");
		stmt.executeUpdate("upsert into employee values (1,'kalyan','dev',10000)");
		stmt.executeUpdate("upsert into employee values (2,'raj','dev',15000)");
		stmt.executeUpdate("upsert into employee values (3,'venkat','test',5000)");
		con.commit();

		PreparedStatement statement = con.prepareStatement("select * from employee");
		rset = statement.executeQuery();
		while (rset.next()) {
			System.out.println(rset.getString("name"));
		}
		statement.close();
		con.close();
	}
}