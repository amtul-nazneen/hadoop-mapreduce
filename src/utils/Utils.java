package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Utils {

	
	public static int toInt(String value)
	{
		if(value!=null)
			return Integer.parseInt(value);
		else
			return -1;
	}
	
	public static boolean validInputPairs(int user1, int user2)
	{
		if (
			(user1==Constants.p1_u1 && user2 ==Constants.p1_u2 )||(user1==Constants.p1_u2 && user2 ==Constants.p1_u1)||
		    (user1==Constants.p2_u1 && user2 ==Constants.p2_u2 )||(user1==Constants.p2_u2 && user2 ==Constants.p2_u1 )||
		    (user1==Constants.p3_u1 && user2 ==Constants.p3_u2 )||(user1==Constants.p3_u2 && user2 ==Constants.p3_u1 )||
		    (user1==Constants.p4_u1 && user2 ==Constants.p4_u2 )||(user1==Constants.p4_u2 && user2 ==Constants.p4_u1 )||
		    (user1==Constants.p5_u1 && user2 ==Constants.p5_u2 )||(user1==Constants.p5_u2 && user2 ==Constants.p5_u1)
		    )
			return true;
		else
			return false;
	}
	
	public static boolean validInputPairsQ3(int user1, int user2, int input1, int input2)
	{
		if (
			(user1==input1 && user2 ==input2 )||(user1==input2 && user2 ==input1)
		    )
			return true;
		else
			return false;
	}
	
	public static String sortedPair(int user1, int user2)
	{
		if (user1 < user2) 
			return user1 + "," + user2;
		 else 
			return user2 + "," + user1;
	}
	
	public static String formatOutput(List<String> values)
	{
		String result="";
		
		for(String value:values)
			result+=value+",";
		
		if (!result.isEmpty())
			result = result.substring(0, result.length() - 1);
		
		return result;
	}
	
	public static String formatOutput(StringBuilder str)
	{
		if (str.length() > 0) {
			str=str.deleteCharAt(str.length() - 1);
			return str.toString();
		}
		else
			return null;
	}
	
	public static int calculateAge(String s) throws ParseException {

		Calendar today = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
		Date date = sdf.parse(s);
		Calendar dob = Calendar.getInstance();
		dob.setTime(date);

		int curYear = today.get(Calendar.YEAR);
		int dobYear = dob.get(Calendar.YEAR);
		int age = curYear - dobYear;

		int curMonth = today.get(Calendar.MONTH);
		int dobMonth = dob.get(Calendar.MONTH);
		if (dobMonth > curMonth) { // this year can't be counted!
			age--;
		} else if (dobMonth == curMonth) { // same month? check for day
			int curDay = today.get(Calendar.DAY_OF_MONTH);
			int dobDay = dob.get(Calendar.DAY_OF_MONTH);
			if (dobDay > curDay) { // this year can't be counted!
				age--;
			}
		}
		return age;
	}
	
}
