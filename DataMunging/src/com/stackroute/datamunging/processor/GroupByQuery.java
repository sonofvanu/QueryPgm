package com.stackroute.datamunging.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.stackroute.datamunging.evaluator.DataSorter;
import com.stackroute.datamunging.evaluator.QueryTypeBasedOperation;
import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.model.HeaderRowData;
import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.parsing.QueryParameter;

public class GroupByQuery implements QueryExecutor {

	@SuppressWarnings("null")
	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		// TODO Auto-generated method stub
		QueryTypeBasedOperation queryTypeBasedOperation = new QueryTypeBasedOperation();
		DataCarrier dataSet = new DataCarrier();
		HeaderRowData headerRow = queryParameter.getHeaderRow();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryParameter.getFilePath()));
		RowDataHolder rowData;
		bufferedReader.readLine();
		String row;
		Set<String> columnNames = headerRow.keySet();
		while ((row = bufferedReader.readLine()) != null) {
			int count = 0;
			rowData = new RowDataHolder();
			String rowValues[] = row.trim().split(",");
			int columnCount = rowValues.length;
			if (!queryParameter.isHasAllColumn()) {
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
			String groupByColumnValue = rowData.get(headerRow.get(queryParameter.getGroupByColumn()));
			List<RowDataHolder> dataValues = null;
			if (queryParameter.isHasWhere()
					&& queryTypeBasedOperation.checkingIfWhereConditionPasses(queryParameter, rowValues)&&queryParameter.isHasGroupBy()) {
					
					if (dataSet.getGroupByDataSetNew().containsKey(groupByColumnValue)) {
						dataValues.add(rowData);
					} else {
						dataValues = new ArrayList<RowDataHolder>();
						dataValues.add(rowData);
					}
			} else {
				dataSet.getResultSet().add(rowData);
				if (queryParameter.isHasGroupBy() && dataSet.getGroupByDataSetNew().containsKey(groupByColumnValue)) {
						dataValues.add(rowData);
				}
					 else {
						dataValues = new ArrayList<RowDataHolder>();
						dataValues.add(rowData);
					}
					
				}
			dataSet.getGroupByDataSetNew().put(groupByColumnValue, dataValues);
		}
		if (queryParameter.isHasOrderBy()) {
			DataSorter sortData = new DataSorter();
			sortData.setSortingIndex(queryParameter.getHeaderRow().get(queryParameter.getOrderByColumn()));
			Collections.sort(dataSet.getResultSet(), sortData);
		}
		bufferedReader.close();
		return dataSet;
	}
}