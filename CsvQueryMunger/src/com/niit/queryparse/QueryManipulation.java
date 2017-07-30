package com.niit.queryparse;

import java.util.Collections;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Set;

import com.niit.model.Restrictions;
import com.niit.processor.DataSorting;
import com.niit.queryoperation.DataSetter;
import com.niit.queryoperation.HeaderData;
import com.niit.queryoperation.RowData;

public class QueryManipulation {
	QuerySegregation querySegregation;

	public QueryManipulation(QuerySegregation query) {
		this.querySegregation = query;
	}

	public DataSetter getCsvData() {
		DataSetter dataSet = new DataSetter();
		HeaderData headerRow;
		BufferedReader bufferedReader;
		RowData rowData;
		try {
			bufferedReader = new BufferedReader(new FileReader(querySegregation.getFilePath()));

			bufferedReader.readLine();

			String row;
			while ((row = bufferedReader.readLine()) != null) {
				int count = 0;
				rowData = new RowData();

				String rowValues[] = row.trim().split(",");
				int columnCount = rowValues.length;

				if (!querySegregation.isHasAllColumn()) {
					headerRow = querySegregation.getHeaderRow();
					Set<String> fileColumnNames = headerRow.keySet();
					for (String queryColumnName : querySegregation.getColumNames().getColumns()) {
						for (String fileColumnName : fileColumnNames) {
							if (queryColumnName.equals(fileColumnName)) {
								rowData.put(headerRow.get(queryColumnName),
										rowValues[headerRow.get(queryColumnName)].trim());
							}
						}
					}
				} else {
					while (count < columnCount) {
						rowData.put(count, rowValues[count].trim());
						count++;
					}
				}

				if (querySegregation.isHasWhere()) {
					if (evaluateWhereCondition(querySegregation, rowValues)) {
						dataSet.getResultSet().add(rowData);
					}
				} else {
					dataSet.getResultSet().add(rowData);
				}
			}

			if (querySegregation.isHasOrderBy()) {
				DataSorting sortData = new DataSorting();
				sortData.setSortingIndex(querySegregation.getHeaderRow().get(querySegregation.getOrderByColumn()));
				Collections.sort(dataSet.getResultSet(), sortData);
			}

			if (querySegregation.isHasAggregate()) {

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataSet;
	}

	private boolean evaluateWhereCondition(QuerySegregation query, String rowValues[]) {
		List<Restrictions> criteariaList = query.getListrelexpr();
		HeaderData headerRow = query.getHeaderRow();
		List<String> logicalOperators = query.getLogicalOperator();

		boolean expression = evaluateCriteria(criteariaList.get(0), rowValues, headerRow);

		int logicalOperatorCount = logicalOperators.size();
		int count = 0;

		if (logicalOperatorCount > 0) {
			while (count < logicalOperatorCount) {
				if (logicalOperators.get(count).equals("and")) {
					expression = expression && evaluateCriteria(criteariaList.get(count + 1), rowValues, headerRow);
				} else {
					expression = expression || evaluateCriteria(criteariaList.get(count + 1), rowValues, headerRow);
				}

				count++;
			}
		}

		return expression;
	}

	private boolean evaluateCriteria(Restrictions criteria, String rowValues[], HeaderData headerRow) {

		boolean expression = false;

		String conditionColumnName = criteria.getColumn();
		String conditionOperator = criteria.getOperator();
		String conditionValue = criteria.getValue();

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
			float parsedConditionValue = Float.parseFloat(conditionValue);
			float parsedRowDataValue = Float.parseFloat(rowValues[querySegregation.getHeaderRow().get(conditionColumnName)]);
			switch (conditionOperator) {
			
			///---switch case for multiple relational operators------////
			case ">=":
				expression = parsedRowDataValue >= parsedConditionValue;
				break;
			case "<=":
				expression = parsedRowDataValue <= parsedConditionValue;
				break;
			case ">":
				expression = parsedRowDataValue > parsedConditionValue;
				break;
			case "<":
				expression = parsedRowDataValue < parsedConditionValue;
				break;
			case "=":
				expression = parsedRowDataValue == parsedConditionValue;
				break;
			case "!=":
				expression = parsedRowDataValue != parsedConditionValue;
				break;
			}
		}

		return expression;
	}

	private boolean evaluateDataType(String value) {
//		try {
//			Float f = Float.parseFloat(value);
//			return false;
//		} catch (Exception e) {
//			return true;
//		}
		
		NumberFormat formatter = NumberFormat.getInstance();
		  ParsePosition pos = new ParsePosition(0);
		  formatter.parse(value, pos);
		  return !(value.length() == pos.getIndex());
}

}
