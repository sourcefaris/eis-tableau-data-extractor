package com.eis.dataextractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.eis.dataextractor.utils.StringDateUtils;
import com.mysql.jdbc.PreparedStatement;

public class LocalDBController {
	private static Connection dbConnection = DBConnection.getConnection();
	
	public static Timestamp getMaxDateTime(String propertyName){
    	Timestamp ts = null;
    	Statement stat = null;
    	String sql = "SELECT date_format(str_to_date(prop.property_value,'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') from properties prop WHERE prop.property_name='"+propertyName+"'";
    	try {
			stat = dbConnection.createStatement();
			ResultSet rs = stat.executeQuery(sql);
			while (rs.next()) {
				ts = StringDateUtils.convertStringToDate(rs.getString(1),null);
				stat.close();
				break;
			}
    	} catch (SQLException ex){
    		ex.printStackTrace();
    	}
    	return ts;
    }
	
	public static void updateProperty(String value, String name ){
		PreparedStatement preparedStatement = null;
		String insertTableSQL = "UPDATE properties SET property_value=? WHERE property_name=?";
		try {
			preparedStatement = (PreparedStatement) dbConnection.prepareStatement(insertTableSQL);
			preparedStatement.setString(1, value);
			preparedStatement.setString(2, name);
			preparedStatement.executeUpdate();
			System.out.println("Record is updated into Properties table!");
			preparedStatement.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static String getPropertyValue(String propertyName){
		String property = null;
    	Statement stat = null;
    	String sql = "SELECT property_value FROM properties WHERE property_name='"+propertyName+"'";
    	try {
			stat = dbConnection.createStatement();
			ResultSet rs = stat.executeQuery(sql);
			while (rs.next()) {
				property = rs.getString(1);
				stat.close();
				break;
			}
    	} catch (SQLException ex){
    		ex.printStackTrace();
    	}
    	return property;
	}
}
