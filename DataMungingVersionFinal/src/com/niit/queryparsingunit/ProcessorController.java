package com.niit.queryparsingunit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.niit.queryprocessingunit.AggregateTypeQueryProcessing;
import com.niit.queryprocessingunit.GroupByTypeProcessing;
import com.niit.queryprocessingunit.MainQueryProcessing;
import com.niit.queryprocessingunit.SimpleTypeQueryProcessing;

public class ProcessorController {
	QueryParser queryParser;
	BufferedReader bufferedReader;

	private Map<String, Integer> getHeaderRows(String fileLocation) {
		Map<String, Integer> headerRows = new HashMap<>();
		try {
			bufferedReader = new BufferedReader(new FileReader(fileLocation));
			String headerLine = bufferedReader.readLine();
			String headerArray[] = headerLine.split(",");
			int headerLength = headerArray.length;
			for (int i = 0; i < headerLength; i++) {
				headerRows.put(headerArray[i], i);
			}

		} catch (IOException io) {

		}

		return headerRows;
	}

	public Map<Integer, String> queryFunctionsCaller(String query) {
		MainQueryProcessing queryProcessor;
		queryParser = new QueryParser();
		Map<Integer, String> finalResultData = null;
		QueryParser queryParserObjectTwo = queryParser.querySplitting(query);
		switch (queryParserObjectTwo.getQueryFormat()) {
		case "groupby":
			queryProcessor = new GroupByTypeProcessing();
			finalResultData = queryProcessor.executeQuery(queryParserObjectTwo,
					getHeaderRows(queryParserObjectTwo.getFileLocation()));
			break;
		case "aggregate":
			queryProcessor = new AggregateTypeQueryProcessing();
			finalResultData = queryProcessor.executeQuery(queryParserObjectTwo,
					getHeaderRows(queryParserObjectTwo.getFileLocation()));
			break;
		case "simple":
			queryProcessor = new SimpleTypeQueryProcessing();
			finalResultData = queryProcessor.executeQuery(queryParserObjectTwo,
					getHeaderRows(queryParserObjectTwo.getFileLocation()));
			break;
		}
		return finalResultData;
	}

}
