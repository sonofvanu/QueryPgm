package com.stackroute.datamunging.processor;

import java.io.*;
import java.util.*;

import com.stackroute.datamunging.model.*;
import com.stackroute.datamunging.parsing.*;

public class AggregateQuery implements QueryExecutor {

	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		QueryTypeBasedOperation queryTypeBasedOperation = new QueryTypeBasedOperation();
		DataCarrier dataSet = new DataCarrier();
		LinkedHashMap<String, Integer> headerRow = queryParameter.getHeaderRow();
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

					if (queryParameter.isHasGroupBy()) {
						String groupByColumnValue = rowData.get(headerRow.get(queryParameter.getGroupByColumn()));
						List<RowDataHolder> dataValues = null;
						if (dataSet.getGroupByDataSetNew().containsKey(groupByColumnValue)) {
							dataValues = dataSet.getGroupByDataSetNew().get(groupByColumnValue);
							dataValues.add(rowData);
						} else {
							dataValues = new ArrayList<RowDataHolder>();
							dataValues.add(rowData);
						}
						dataSet.getGroupByDataSetNew().put(groupByColumnValue, dataValues);
					}
				}
			} else {
				dataSet.getResultSet().add(rowData);

				if (queryParameter.isHasGroupBy()) {
					String groupByColumnValue = rowData.get(headerRow.get(queryParameter.getGroupByColumn()));
					List<RowDataHolder> dataValues = null;
					if (dataSet.getGroupByDataSetNew().containsKey(groupByColumnValue)) {
						dataValues = dataSet.getGroupByDataSetNew().get(groupByColumnValue);
						dataValues.add(rowData);
					} else {
						dataValues = new ArrayList<RowDataHolder>();
						dataValues.add(rowData);
					}
					dataSet.getGroupByDataSetNew().put(groupByColumnValue, dataValues);
				}
			}

		}

		if (queryParameter.isHasOrderBy()) {
			DataSorter sortData = new DataSorter();
			sortData.setSortingIndex(queryParameter.getHeaderRow().get(queryParameter.getOrderByColumn()));
			Collections.sort(dataSet.getResultSet(), sortData);
		}

		if (queryParameter.isHasAggregate()) {
			if (queryParameter.isHasGroupBy()) {
				LinkedHashMap<String, List<RowDataHolder>> groupDataSet = dataSet.getGroupByDataSetNew();
				Set<String> groupByColumnValues = groupDataSet.keySet();
				LinkedHashMap<String, Float> eachGroupAggregateRow;
				for (String groupByColumnValue : groupByColumnValues) {
					List<RowDataHolder> groupRows = dataSet.getGroupByDataSetNew().get(groupByColumnValue);
					eachGroupAggregateRow = queryTypeBasedOperation.checkingIfAgrregateConditionPasses(groupRows,
							queryParameter);
					dataSet.getTotalGroupedData().put(groupByColumnValue, eachGroupAggregateRow);
				}

			} else {
				dataSet.setAggregateRow(queryTypeBasedOperation
						.checkingIfAgrregateConditionPasses(dataSet.getResultSet(), queryParameter));
			}
		}

		return dataSet;
	}
}
