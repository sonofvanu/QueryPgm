package com.niit.queryparse;
import com.niit.queryoperation.DataSetter;
public class QueryChecking {
	public DataSetter executeQuery(String queryString) 
	{
		if (isValidQueryString(queryString)) 
		{
			QuerySegregation query=null;
			try {
				query = new QuerySegregation(queryString);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return new QueryManipulation(query).getCsvData();
		} 
		
		return null;
		
	}

	private boolean isValidQueryString(String queryString) 
	{
		if(queryString.contains("select") && queryString.contains("from") || (queryString.contains("where") ||queryString.contains("order by")|| queryString.contains("group by")))
		{
			return true;
		}
		else
		{
			return false;
		}
	}


}
