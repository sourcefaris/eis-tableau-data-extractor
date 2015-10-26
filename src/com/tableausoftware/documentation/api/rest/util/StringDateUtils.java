package com.tableausoftware.documentation.api.rest.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class StringDateUtils {
	private static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	public static Date convertStringToDate(String dateTime){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);
        Date date = null;
        try
        {
            date = simpleDateFormat.parse(dateTime);
            return date;
        }
        catch (ParseException ex)
        {
            System.out.println("Exception "+ex);
        }
        return null;
	}
	
	public static Timestamp convertStringToDate(String datestr, String x){
		Date date = convertStringToDate(datestr);
		return new Timestamp(date.getYear(), date.getMonth(), date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds(), 0);
	}
		
	public static Date getMaxDate(List<String> dates){
		Date comparator = new Date(0, 0, 0, 0, 0, 0);
		for(int i=0;i<dates.size();i++){
			Timestamp date = convertStringToDate(dates.get(i),null);
			if(comparator.before(date)){
				comparator = date;
			}
		}
		return comparator;
	}
	
	public static String convertDateToString(Date date){
		DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
		return dateFormat.format(date);
	}
	
	public static void main(String[] args) {
		System.out.println(StringDateUtils.convertStringToDate("2012-12-10 10:00:00"));
		System.out.println(StringDateUtils.convertDateToString(new Timestamp(System.currentTimeMillis())));
	}
}
