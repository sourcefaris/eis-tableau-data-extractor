package com.eis.dataextractor;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.log4j.BasicConfigurator;


public class DBConnection {
	private static Connection dbConnection = null;
	private static Properties PROP = new Properties();
	static {
        BasicConfigurator.configure();
        try {
            PROP.load(new FileInputStream("res/config.properties"));
        } catch (IOException e) {
            System.out.println("Failed to load configuration files.");
            e.printStackTrace();
        }
    }
	public static Connection getConnection() {
		if(dbConnection==null){
			try {
				Class.forName(PROP.getProperty("local.db.driver"));
				dbConnection = DriverManager.getConnection(
						PROP.getProperty("local.db.connection"), PROP.getProperty("local.db.username"),PROP.getProperty("local.db.password"));
				return dbConnection;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			} catch (ClassNotFoundException e) {
				System.out.println(e.getMessage());
			}
		}
		return dbConnection;
	}
}
