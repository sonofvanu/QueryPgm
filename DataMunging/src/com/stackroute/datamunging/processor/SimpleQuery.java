package com.stackroute.datamunging.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;
import com.stackroute.datamunging.evaluator.QueryTypeBasedOperation;
import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.model.HeaderRowData;
import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.parsing.QueryParameter;

public class SimpleQuery implements QueryExecutor {

	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		QueryTypeBasedOperation queryTypeBasedOperation = new QueryTypeBasedOperation();
		DataCarrier dataCarrier = new DataCarrier();
		HeaderRowData headerRowData = queryParameter.getHeaderRow();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryParameter.getFilePath()));
		RowDataHolder rowDataHolder;
		bufferedReader.readLine();
		String row;
		Set<String> columnNames = headerRowData.keySet();
		while ((row = bufferedReader.readLine()) != null) {
			rowDataHolder = new RowDataHolder();
			String rowValues[] = row.trim().split(",");
			int columnCount = rowValues.length, counter = 0;
			if (!queryParameter.isHasAllColumn()) {
				for (String columnName : queryParameter.getColumNames().getColumns()) {
					for (String actualColumnName : columnNames) {
						if (actualColumnName.equals(columnName)) {
							rowDataHolder.put(headerRowData.get(columnName),
									rowValues[headerRowData.get(columnName)].trim());
						}
					}
				}
			} else {
				while (counter < columnCount) {
					rowDataHolder.put(counter, rowValues[counter].trim());
					counter++;
				}
			}
			if (queryParameter.isHasWhere()
					&& queryTypeBasedOperation.checkingIfWhereConditionPasses(queryParameter, rowValues)) {
		dataCarrier.getResultSet().add(rowDataHolder);

			} else {
				dataCarrier.getResultSet().add(rowDataHolder);
			}
		}
		bufferedReader.close();
		return dataCarrier;
	}
}
