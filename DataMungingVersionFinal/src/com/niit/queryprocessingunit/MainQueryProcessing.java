package com.niit.queryprocessingunit;

import java.util.Map;

import com.niit.queryparsingunit.QueryParser;

public interface MainQueryProcessing {
public Map<Integer,String> executeQuery(QueryParser queryParser,Map<String,Integer> headerData);
}
