package com.stackroute.datamunger.query;

//class to check the query and execute the query
public class QueryEligibilityQualifier 
{
	public DataCarrier executeQuery(String queryString) throws Exception
	{
		if (isValidQueryString(queryString)) 
		{ 
			
			QueryParseContainer queryParam=new QueryParseContainer(queryString);
			QueryProcessor queryProcessor=new QueryProcessor(queryParam);
			return queryProcessor.getDataSetSimpleQuery();
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
