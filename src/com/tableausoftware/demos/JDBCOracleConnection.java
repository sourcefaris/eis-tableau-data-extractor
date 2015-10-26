package com.tableausoftware.demos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCOracleConnection {
	public static void main(String[] args) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = null;
			connection = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/hadooptest","root", "");
			connection.close();
			System.out.println("Connection success");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
