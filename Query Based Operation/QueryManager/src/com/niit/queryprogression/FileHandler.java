package com.niit.queryprogression;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileHandler {
	QuerySplitter querySplitter = new QuerySplitter();
	QueryProgression queryProgression = new QueryProgression();
	BufferedReader bufferedReader;

	public Map<Integer, String> executeQuery(String query) {

		QuerySplitter parsedQuery = querySplitter.validateThruRegex(query);
		if (parsedQuery.getType4() == 4) {
			return queryProgression.groupByQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
		}

		if (parsedQuery.getType3() == 3) {
			if (parsedQuery.getType2() == 2) {
				return queryProgression.orderByWhereQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			} else {
				if (parsedQuery.getCondition().size() == 1) {
					return queryProgression.getOrderByDataForAllRows(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
				} else {
					return queryProgression.getOrderByForSpecifiedCols(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
				}
			}
		}
		if (parsedQuery.getType2() == 2) {
			if (parsedQuery.getCondition().get(0).equals("*"))
				return queryProgression.whereQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));

			else if (parsedQuery.isHasAggregate()) {
				return queryProgression.aggregateWithWhere(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			} else {
				return queryProgression.multiWhereProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			}
		}
		if (parsedQuery.getType1() == 1) {
			if (parsedQuery.getCols().get(0).equals("*"))
				return getDataFromCsv(parsedQuery);
			else if (parsedQuery.isHasAggregate()) {
				Map<Integer,String> rowdata=new HashMap<>();
				Map<Integer,Aggregators> r=queryProgression.aggregateQuery(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
				int i=0;
				for(Aggregators a:r.values()){
					rowdata.put(i,a.toString());
					i++;
				}
				return rowdata;
			} else {
				return queryProgression.specifiedQueryProcessor(parsedQuery, storeHeaderOfCsv(parsedQuery.getPath()));
			}

		}

		return null;
	}

	private Map<String, Integer> storeHeaderOfCsv(String csvpath) {
		Map<String, Integer> headerInMap = new HashMap<>();
		try {
			bufferedReader = new BufferedReader(new FileReader(csvpath));
			String firstline = bufferedReader.readLine();
			String headerarr[] = firstline.split(",");
			int len = headerarr.length;
			for (int i = 0; i < len; i++) {
				headerInMap.put(headerarr[i], i);
			}

		} catch (IOException io) {

		}

		return headerInMap;
	}

	private Map<Integer, String> getDataFromCsv(QuerySplitter querySplitter) {
		Map<Integer, String> rowdata = new HashMap<>();
		try {
			bufferedReader = new BufferedReader(new FileReader(querySplitter.getPath()));

			int index = 0;
			String str = bufferedReader.readLine();
			while (str != null) {
				rowdata.put(index, str);
				index++;
				str = bufferedReader.readLine();
			}
		}

		catch (IOException io) {

		}
		return rowdata;
	}

}