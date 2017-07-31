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

public class AggregateQuery implements QueryExecutor {

	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		// TODO Auto-generated method stub
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
					eachGroupAggregateRow = queryEvaluator.evaluateAggregateColumns(groupRows, queryParameter);
					dataSet.getTotalGroupedData().put(groupByColumnValue, eachGroupAggregateRow);
				}

			} else {
				dataSet.setAggregateRow(
						queryEvaluator.evaluateAggregateColumns(dataSet.getResultSet(), queryParameter));
			}
		}

		return dataSet;
	}

}
