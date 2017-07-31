package com.stackroute.datamunging.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.stackroute.datamunging.evaluator.QueryEvaluator;
import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.model.HeaderRowData;
import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.parsing.DataSorter;
import com.stackroute.datamunging.parsing.QueryParameter;
import com.stackroute.datamunging.parsing.QueryParser;

public class SimpleQuery implements QueryExecutor {

	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		QueryEvaluator queryEvaluator = new QueryEvaluator();
		DataCarrier dataSet = new DataCarrier();

		HeaderRowData headerRow = queryParameter.getHeaderRow();

		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryParameter.getFilePath()));
		RowDataHolder rowData;
		bufferedReader.readLine();
		String row;
		while ((row = bufferedReader.readLine()) != null) {
			int count = 0;
			rowData = new RowDataHolder();

			String rowValues[] = row.trim().split(",");
			int columnCount = rowValues.length;

			if (!queryParameter.isHasAllColumn()) {
				Set<String> columnNames = headerRow.keySet();
				for (String columnName : queryParameter.getColumNames().getColumns()) {
					for (String actualColumnName : columnNames) {
						if (actualColumnName.equals(columnName)) {
							rowData.put(headerRow.get(columnName), rowValues[headerRow.get(columnName)].trim());
						}
					}
				}
			} else {
				while (count < columnCount) {
					rowData.put(count, rowValues[count].trim());
					count++;
				}
			}

			if (queryParameter.isHasWhere()) {
				if (queryEvaluator.evaluateWhereCondition(queryParameter, rowValues)) {
					dataSet.getResultSet().add(rowData);

				}
			} else {
				dataSet.getResultSet().add(rowData);

			}

		}

		return dataSet;
	}

}
