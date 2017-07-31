package com.stackroute.datamunging.evaluator;

import java.util.LinkedHashMap;
import java.util.List;

import com.stackroute.datamunging.model.HeaderRowData;
import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.model.WhereRestrictionalConditions;
import com.stackroute.datamunging.parsing.QueryParameter;

public class QueryTypeBasedOperation {

QueryParameter queryParam;

	@SuppressWarnings("unused")
	public LinkedHashMap<String, Float> evaluateAggregateColumns(List<RowDataHolder> groupOfRowData,
			QueryParameter queryParameter) {
		LinkedHashMap<String, Float> groupingRowValues = new LinkedHashMap<String, Float>();
		this.queryParam=queryParameter;
		int numberOfAggregateColumns = queryParameter.getColumNames().getColumns().size();
		List<String> actualAggregateColumns = queryParameter.getColumNames().getColumns();
		int valuecounter = 0, dataCounter = 0, columnCounter = 0,rowGroupSize=groupOfRowData.size();
		float sumOfValues = 0, averageOfValues = 0, minimumOfValues = 0, maximumOfValues = 0, countingNumberOfValues = 0, countingNumberOfColumns = 0;

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

					if (!evaluateDataType(actualRowValue)) {
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
					}

					else if (aggregateColumnString.contains("count(")) {
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

	public boolean evaluateWhereCondition(QueryParameter queryParam, String rowValues[]) {
		List<WhereRestrictionalConditions> listRelationalExpression = queryParam
				.getRestrictions();
		HeaderRowData headerRow = queryParam.getHeaderRow();
		List<String> logicalOperators = queryParam.getLogicalOperator();

		boolean expression = evaluateEachCondition(listRelationalExpression.get(0), rowValues, headerRow);

		int logicalOperatorCount = logicalOperators.size();
		int count = 0;

		if (logicalOperatorCount > 0) {
			while (count < logicalOperatorCount) {
				if (logicalOperators.get(count).equals("and")) {
					expression = expression
							&& evaluateEachCondition(listRelationalExpression.get(count + 1), rowValues, headerRow);
				} else {
					expression = expression
							|| evaluateEachCondition(listRelationalExpression.get(count + 1), rowValues, headerRow);
				}

				count++;
			}
		}

		return expression;
	}

	private boolean evaluateEachCondition(WhereRestrictionalConditions condition, String rowValues[],
			HeaderRowData headerRow) {

		boolean expression = false;

		String conditionColumnName = condition.getColumn().trim().toLowerCase();
		String conditionOperator = condition.getOperator();
		String conditionValue = condition.getValue();

		String columnValue = rowValues[headerRow.get(conditionColumnName)].trim();
		boolean isString = evaluateDataType(columnValue);

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
			float conditionParseValue = Float.parseFloat(conditionValue);
			float rowDataValue = Float.parseFloat(rowValues[headerRow.get(conditionColumnName)]);
			switch (conditionOperator) {
			case ">=":
				expression = rowDataValue >= conditionParseValue;
				break;
			case "<=":
				expression = rowDataValue <= conditionParseValue;
				break;
			case ">":
				expression = rowDataValue > conditionParseValue;
				break;
			case "<":
				expression = rowDataValue < conditionParseValue;
				break;
			case "=":
				expression = rowDataValue == conditionParseValue;
				break;
			case "!=":
				expression = rowDataValue != conditionParseValue;
				break;
			}
		}

		return expression;
	}

	private boolean evaluateDataType(String value) {
		try {
			@SuppressWarnings("unused")
			Float f = Float.parseFloat(value);
			return false;
		} catch (Exception e) {
			return true;
		}
	}

}
