package com.niit.queryprogression;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class FileHandler {
	QueryParsingUnit queryParse = new QueryParsingUnit();
	QueryExecutor processor = new QueryExecutor();
	BufferedReader reader;

	public Map<Integer, String> executeQuery(String query) {

		QueryParsingUnit parsedQuery = queryParse.validateThruRegex(query);
		// storeHeaderOfCsv(parsedQuery.getPath());//header from csv
		// getDataFromCsv(parsedQuery);//content from csv
		if (parsedQuery.getType4() == 4) {
			return processor.groupByQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
		}

		if (parsedQuery.getType3() == 3) {
			if (parsedQuery.getType2() == 2) {
				return processor.orderByWhereQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			} else {
				if (parsedQuery.getCondition().size() == 1) {
					return processor.getOrderByDataForAllRows(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
				} else {
					return processor.getOrderByForSpecifiedCols(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
				}
			}
		}
		if (parsedQuery.getType2() == 2) {
			if (parsedQuery.getCondition().get(0).equals("*"))
				return processor.whereQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));

			else if (parsedQuery.isHasAggregate()) {
				return processor.aggregateWithWhere(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			} else {
				return processor.multiWhereProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			}
		}
		if (parsedQuery.getType1() == 1) {
			if (parsedQuery.getCols().get(0).equals("*"))
				return getDataFromCsv(parsedQuery);
			else if (parsedQuery.isHasAggregate()) {
				Map<Integer,String> rowdata=new HashMap<>();
				Map<Integer,Aggregate> r=processor.aggregateQuery(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
				int i=0;
				for(Aggregate a:r.values()){
					rowdata.put(i,a.toString());
					i++;
				}
				return rowdata;
			} else {
				return processor.specifiedQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			}

		}

		return null;
	}

	private Map<String, Integer> storeHeaderOfCsv(String csvpath) {
		Map<String, Integer> headerInMap = new HashMap<>();
		try {
			reader = new BufferedReader(new FileReader(csvpath));
			String firstline = reader.readLine();
			String headerarr[] = firstline.split(",");
			int len = headerarr.length;
			for (int i = 0; i < len; i++) {
				headerInMap.put(headerarr[i], i);
			}

		} catch (IOException io) {

		}

		return headerInMap;
	}

	private Map<Integer, String> getDataFromCsv(QueryParsingUnit queryparse) {
		Map<Integer, String> rowdata = new HashMap<>();
		try {
			reader = new BufferedReader(new FileReader(queryparse.getPath()));

			int index = 0;
			String str = reader.readLine();
			while (str != null) {
				rowdata.put(index, str);
				index++;
				str = reader.readLine();
			}
		}

		catch (IOException io) {

		}
		return rowdata;
	}

}