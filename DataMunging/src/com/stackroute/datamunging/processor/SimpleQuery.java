package com.stackroute.datamunging.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import com.stackroute.datamunging.model.DataCarrier;

import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.model.WhereRestrictionalConditions;
import com.stackroute.datamunging.parsing.QueryParameter;

public class SimpleQuery implements QueryExecutor {

	@Override
	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception {
		// TODO Auto-generated method stub
		/*QueryTypeBasedOperation queryTypeBasedOperation = new QueryTypeBasedOperation();*/
		DataCarrier dataCarrier = new DataCarrier();
		HashMap<String,Integer> headerRowData = queryParameter.getHeaderRow();
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
				for (String columnName : queryParameter.getColumNames()) {
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
					&& this.checkingIfWhereConditionPasses(queryParameter, rowValues)) {
				dataCarrier.getResultSet().add(rowDataHolder);

			} else {
				dataCarrier.getResultSet().add(rowDataHolder);
			}
		}
		bufferedReader.close();
		return dataCarrier;
	}
	
	
	
	
	public boolean checkingIfWhereConditionPasses(QueryParameter queryParam, String rowValues[]) {
		List<WhereRestrictionalConditions> listRelationalExpression = queryParam.getRestrictions();
		HashMap<String,Integer> headerRow = queryParam.getHeaderRow();
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

	

	private boolean checkingOfEveryConditionSent(WhereRestrictionalConditions condition, String rowValues[],
			HashMap<String,Integer> headerRow) {
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
