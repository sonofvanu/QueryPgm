package com.stackroute.datamunging.processor;

import java.util.LinkedHashMap;
import java.util.List;

import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.model.WhereRestrictionalConditions;
import com.stackroute.datamunging.parsing.QueryParameter;

public class QueryTypeBasedOperation {
	public boolean checkingIfWhereConditionPasses(QueryParameter queryParam, String rowValues[]) {
		List<WhereRestrictionalConditions> listRelationalExpression = queryParam.getRestrictions();
		LinkedHashMap<String, Integer> headerRow = queryParam.getHeaderRow();
		List<String> logicalOperators = queryParam.getLogicalOperator();
		boolean expression = checkingIfEveryWhereConditionPasses(listRelationalExpression.get(0), rowValues, headerRow);
		int logicalOperatorCount = logicalOperators.size();
		int count = 0;
		if (logicalOperatorCount > 0) {
			while (count < logicalOperatorCount) {
				if (logicalOperators.get(count).equals("and")) {
					expression = expression && checkingIfEveryWhereConditionPasses(
							listRelationalExpression.get(count + 1), rowValues, headerRow);
				} else {
					expression = expression || checkingIfEveryWhereConditionPasses(
							listRelationalExpression.get(count + 1), rowValues, headerRow);
				}
				count++;
			}
		}
		return expression;
	}

	private boolean checkingIfEveryWhereConditionPasses(WhereRestrictionalConditions condition, String rowValues[],
			LinkedHashMap<String, Integer> headerRow) {
		boolean expression = false;
		String conditionColumnName = condition.getColumn().trim().toLowerCase();
		String conditionOperator = condition.getOperator();
		String conditionValue = condition.getValue();
		String columnValue = rowValues[headerRow.get(conditionColumnName)].trim();
		boolean isString;
		try {
			Float.parseFloat(columnValue);
			isString = false;
		} catch (Exception e) {
			isString = true;
		}
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
				expression = (rowDataValueParsed >= conditionValueParsed);
				break;
			case "<=":
				expression = (rowDataValueParsed <= conditionValueParsed);
				break;
			case ">":
				expression = (rowDataValueParsed > conditionValueParsed);
				break;
			case "<":
				expression = (rowDataValueParsed < conditionValueParsed);
				break;
			case "=":
				expression = (rowDataValueParsed == conditionValueParsed);
				break;
			case "!=":
				expression = (rowDataValueParsed != conditionValueParsed);
				break;
			}
		}
		return expression;
	}

	public LinkedHashMap<String, Float> checkingIfAgrregateConditionPasses(List<RowDataHolder> groupOfRowData,
			QueryParameter queryParameter) {
		LinkedHashMap<String, Float> groupingRowValues = new LinkedHashMap<String, Float>();
		int numberOfAggregateColumns = queryParameter.getColumNames().size();
		List<String> actualAggregateColumns = queryParameter.getColumNames();
		int valuecounter = 0, columnCounter = 0, rowGroupSize = groupOfRowData.size();
		float sumOfValues = 0, minimumOfValues = 0, maximumOfValues = 0, countingNumberOfValues = 0,
				countingNumberOfColumns = 0;
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
					boolean isString;
					try {
						Float.parseFloat(actualRowValue);
						isString = false;
					} catch (Exception e) {
						isString = true;
					}
					if (!isString) {
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

}
