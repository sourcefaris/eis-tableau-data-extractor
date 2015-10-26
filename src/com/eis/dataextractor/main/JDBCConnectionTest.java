package com.eis.dataextractor.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnectionTest {
	public static void main(String[] args) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection connection = null;
			connection = DriverManager.getConnection(
				"jdbc:hive2://192.168.0.101:22/hadooptest","mapr", "mapr2015");
			connection.close();
			System.out.println("Connection success");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
