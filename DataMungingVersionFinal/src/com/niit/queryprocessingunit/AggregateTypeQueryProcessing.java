package com.niit.queryprocessingunit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.niit.queryhelpingunit.ReteriveData;
import com.niit.querymodelunit.Aggregate;
import com.niit.queryparsingunit.QueryParser;

public class AggregateTypeQueryProcessing implements MainQueryProcessing {

	@Override
	public Map<Integer, String> executeQuery(QueryParser parsedQuery, Map<String, Integer> header) {
		Aggregate aggregate = new Aggregate();
		int columnNameListLength = parsedQuery.getColumnNameList().size();
		int startPoint = 0;
		String aggregateColumns[] = new String[columnNameListLength];
		String aggregateFunctions[] = new String[columnNameListLength];
		for (int i = 0; i < columnNameListLength; i++) {
			Pattern pattern = Pattern.compile("\\((.*?)\\)");
			Matcher matcher = pattern.matcher(parsedQuery.getColumnNameList().get(i));
			if (matcher.find()) {

				aggregateColumns[i] = matcher.group(1);
			}
			aggregateFunctions[i] = parsedQuery.getColumnNameList().get(i).split(aggregateColumns[i])[0];
		}

		Map<Integer, String> aggregateFilteredData = null;

		try {
			QueryParser queryParserObject = (QueryParser) parsedQuery.clone();
			for (int l = 0; l < columnNameListLength; l++) {
				queryParserObject.getColumnNameList().set(l, aggregateColumns[l]);
			}
			aggregateFilteredData = new ReteriveData().csvFileReading(queryParserObject, header);
		} catch (Exception e) {
		}

		Map<Integer, String> rowWiseData = new HashMap<>();
		Map<Integer, Aggregate> requiredDataSet = new HashMap<>();

		int sumOfValues[] = new int[columnNameListLength];
		String string[] = new String[columnNameListLength];
		for (int l = 0; l < columnNameListLength; l++) {
			if (aggregateFunctions[l].contains("sum")) {

				for (String fileteredData : aggregateFilteredData.values()) {
					string[l] = fileteredData;
					sumOfValues[l] += Integer.parseInt(string[l]);
				}
				aggregate.setAggregateFunction("sum");
				aggregate.setColumn(aggregateColumns[l]);
				aggregate.setResult(sumOfValues[l]);
				requiredDataSet.put(++startPoint, aggregate);

			}

			// rowdata.put(index, sum);
			startPoint++;
		}

		int i = 0;
		for (Aggregate aaggregateObj : requiredDataSet.values()) {
			rowWiseData.put(i, aaggregateObj.toString());
			i++;
		}
		return rowWiseData;
	}

}
