package com.eis.dataextractor.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.eis.dataextractor.LocalDBController;

public class JDBCConnectionTest extends LocalDBController {
	public static void main(String[] args) {
		try {
			Class.forName(getPropertyValue("hadoop.driver"));
			Connection connection = null;
			connection = DriverManager.getConnection(
				getPropertyValue("hadoop.connection"),null , null);
			connection.close();
			System.out.println("Connection success");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
