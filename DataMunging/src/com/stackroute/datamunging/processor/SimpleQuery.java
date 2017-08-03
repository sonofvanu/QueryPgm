package com.stackroute.datamunging.processor;

import java.io.*;
import java.util.*;

import com.stackroute.datamunging.model.*;
import com.stackroute.datamunging.parsing.*;

public class SimpleQuery implements QueryExecutor {
	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		QueryTypeBasedOperation queryTypeBasedOperation = new QueryTypeBasedOperation();
		DataCarrier dataSet = new DataCarrier();
		LinkedHashMap<String, Integer> headerRow = queryParameter.getHeaderRow();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryParameter.getFilePath()));
		RowDataHolder rowData;
		bufferedReader.readLine();
		String row;
		Set<String> columnNames = headerRow.keySet();
		while ((row = bufferedReader.readLine()) != null) {
			int count = 0;
			rowData = new RowDataHolder();
			String rowValues[] = row.trim().split(",");
			//-----//
			int columnCount = rowValues.length;
			if (!queryParameter.isHasAllColumn()) {
				for (String columnName : queryParameter.getColumNames()) {
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
				if (queryTypeBasedOperation.checkingIfWhereConditionPasses(queryParameter, rowValues)) {
					dataSet.getResultSet().add(rowData);
				}
			} else {
				dataSet.getResultSet().add(rowData);
			}
		}
		return dataSet;
	}
}
