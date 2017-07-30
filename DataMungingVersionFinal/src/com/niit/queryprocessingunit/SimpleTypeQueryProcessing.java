package com.niit.queryprocessingunit;

import java.util.Map;

import com.niit.queryhelpingunit.OrderData;
import com.niit.queryhelpingunit.ReteriveData;
import com.niit.queryparsingunit.QueryParser;



public class SimpleTypeQueryProcessing implements MainQueryProcessing {
ReteriveData retrieveData;
	@Override
	public Map<Integer, String> executeQuery(QueryParser queryParser, Map<String, Integer> headerData) {
	retrieveData=new ReteriveData();
	if(queryParser.isOrderBy()){
		return new OrderData().resultantDataOrdering(queryParser,retrieveData.csvFileReading(queryParser, headerData),headerData);	
		}

	return retrieveData.csvFileReading(queryParser, headerData);
	}
}