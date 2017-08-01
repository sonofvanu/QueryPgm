package com.stackroute.datamunging.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.stackroute.datamunging.evaluator.DataSorter;

import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.model.WhereRestrictionalConditions;
import com.stackroute.datamunging.parsing.QueryParameter;

public class GroupByQuery implements QueryExecutor {
	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		// TODO Auto-generated method stub
		DataCarrier dataSet = new DataCarrier();
		HashMap<String, Integer> headerRow = queryParameter.getHeaderRow();
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
			String groupByColumnValue = rowData.get(headerRow.get(queryParameter.getGroupByColumn()));
			List<RowDataHolder> dataValues = null;
			if (queryParameter.isHasWhere() && this.checkingIfWhereConditionPasses(queryParameter, rowValues)
					&& queryParameter.isHasGroupBy()) {
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
				} else {
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

	public boolean checkingIfWhereConditionPasses(QueryParameter queryParam, String rowValues[]) {
		List<WhereRestrictionalConditions> listRelationalExpression = queryParam.getRestrictions();
		HashMap<String, Integer> headerRow = queryParam.getHeaderRow();
		List<String> logicalOperators = queryParam.getLogicalOperator();
		boolean expression = checkingOfEveryConditionSent(listRelationalExpression.get(0), rowValues, headerRow);
		int logicalOperatorCount = logicalOperators.size();
		int count = 0;
		if (logicalOperatorCount > 0) {
			while (count < logicalOperatorCount) {
				if (logicalOperators.get(count).equals("and")) {
					expression = expression && checkingOfEveryConditionSent(listRelationalExpression.get(count + 1),
							rowValues, headerRow);
				} else {
					expression = expression || checkingOfEveryConditionSent(listRelationalExpression.get(count + 1),
							rowValues, headerRow);
				}
				count++;
			}
		}
		return expression;
	}

	@SuppressWarnings("unused")
	public LinkedHashMap<String, Float> checkingIfAgrregateConditionPasses(List<RowDataHolder> groupOfRowData,
			QueryParameter queryParameter) {
		LinkedHashMap<String, Float> groupingRowValues = new LinkedHashMap<String, Float>();
		int numberOfAggregateColumns = queryParameter.getColumNames().size();
		List<String> actualAggregateColumns = queryParameter.getColumNames();
		int valuecounter = 0, dataCounter = 0, columnCounter = 0, rowGroupSize = groupOfRowData.size();
		float sumOfValues = 0, averageOfValues = 0, minimumOfValues = 0, maximumOfValues = 0,
				countingNumberOfValues = 0, countingNumberOfColumns = 0;
		while (valuecounter < rowGroupSize) {
			columnCounter = 0;
			while (columnCounter < numberOfAggregateColumns) {
				String aggregateColumnString = actualAggregateColumns.get(columnCounter);
				String aggregateColumnName, actualRowValue;
				if (!aggregateColumnString.equals(queryParameter.getGroupByColumn())) {
					aggregateColumnName = aggregateColumnString.substring(aggregateColumnString.indexOf('(') + 1,
							aggregateColumnString.indexOf(')'));
					actualRowValue = groupOfRowData.get(valuecounter)
							.get(queryParameter.getHeaderRow().get(aggregateColumnName));
					if (!checkingForTheDataType(actualRowValue)) {
						float rowValue = Float.parseFloat(actualRowValue);
						if (valuecounter == 0) {
							minimumOfValues = Float.parseFloat(actualRowValue);
							maximumOfValues = Float.parseFloat(actualRowValue);
						}
						if (aggregateColumnString.contains("sum(")) {
							if (groupingRowValues.containsKey(aggregateColumnString)) {
								sumOfValues = groupingRowValues.get(aggregateColumnString);
								sumOfValues = sumOfValues + rowValue;
								groupingRowValues.put(aggregateColumnString, sumOfValues);
							} else {
								groupingRowValues.put(aggregateColumnString, rowValue);
							}
						} else if (aggregateColumnString.contains("min(")) {
							if (minimumOfValues > rowValue) {
								minimumOfValues = rowValue;
							}
							groupingRowValues.put(aggregateColumnString, minimumOfValues);
						} else if (aggregateColumnString.contains("max(")) {
							if (maximumOfValues < rowValue) {
								maximumOfValues = rowValue;
							}
							groupingRowValues.put(aggregateColumnString, maximumOfValues);
						}
					} else if (aggregateColumnString.contains("count(*)")) {
						countingNumberOfValues++;
						groupingRowValues.put(aggregateColumnString, countingNumberOfValues);
					} else if (aggregateColumnString.contains("count(")) {
						countingNumberOfColumns++;
						groupingRowValues.put(aggregateColumnString, countingNumberOfColumns);
					}
				}
				columnCounter++;
			}
			valuecounter++;
		}
		return groupingRowValues;
	}

	private boolean checkingOfEveryConditionSent(WhereRestrictionalConditions condition, String rowValues[],
			HashMap<String, Integer> headerRow) {
		boolean expression = false;
		String conditionColumnName = condition.getColumn().trim().toLowerCase();
		String conditionOperator = condition.getOperator();
		String conditionValue = condition.getValue();
		String columnValue = rowValues[headerRow.get(conditionColumnName)].trim();
		boolean isString = checkingForTheDataType(columnValue);
		if (isString) {
			if (conditionOperator.equals("=")) {
				if (columnValue.equals(conditionValue)) {
					expression = true;
				}
			} else if (conditionOperator.equals("!=")) {
				if (!columnValue.equals(conditionValue)) {
					expression = true;
				}
			}
		} else {
			float conditionValueParsed = Float.parseFloat(conditionValue);
			float rowDataValueParsed = Float.parseFloat(rowValues[headerRow.get(conditionColumnName)]);
			switch (conditionOperator) {
			case ">=":
				expression = rowDataValueParsed >= conditionValueParsed;
				break;
			case "<=":
				expression = rowDataValueParsed <= conditionValueParsed;
				break;
			case ">":
				expression = rowDataValueParsed > conditionValueParsed;
				break;
			case "<":
				expression = rowDataValueParsed < conditionValueParsed;
				break;
			case "=":
				expression = rowDataValueParsed == conditionValueParsed;
				break;
			case "!=":
				expression = rowDataValueParsed != conditionValueParsed;
				break;
			}
		}
		return expression;
	}

	private boolean checkingForTheDataType(String value) {
		try {
			@SuppressWarnings("unused")
			Float f = Float.parseFloat(value);
			return false;
		} catch (Exception e) {
			return true;
		}
	}
}