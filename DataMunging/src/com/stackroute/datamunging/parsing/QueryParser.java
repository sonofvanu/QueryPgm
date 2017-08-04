package com.stackroute.datamunging.parsing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.stackroute.datamunging.model.WhereRestrictionalConditions;

public class QueryParser {
	QueryParameter queryParameter = new QueryParameter();

	public QueryParameter querySegregator(String queryString) throws Exception {
		String conditionalPartOfQuery = null, selectedColumnOfQuery = null;
		queryParameter.setFilePath((queryString.split("from")[1].trim()).split(" ")[0].trim());
		selectedColumnOfQuery = (queryString.split("from")[0].trim()).split("select")[1].trim();
		if (queryString.contains("order by")) {
			queryParameter.setOrderByColumn((queryString.split("order by")[1].trim()).split(" ")[0].trim().toLowerCase());
			queryParameter.setHasOrderBy(true);
		}
		if (queryString.contains("group by")) {
			queryParameter.setGroupByColumn((queryString.split("group by")[1].trim()).split(" ")[0].trim().toLowerCase());
			queryParameter.setHasGroupBy(true);
		}
		if (queryString.contains("where")) {
			conditionalPartOfQuery = (queryString.split("order by|group by")[0].trim()).split("where")[1].trim();
			this.whereFieldsSeparator(conditionalPartOfQuery);
			queryParameter.setHasWhere(true);
		}
		else 
			queryParameter.setHasSimpleQuery(true);
		this.fieldSeparation(selectedColumnOfQuery);
		queryParameter.setHeaderRow(this.setHeaderRow());
		return queryParameter;
	}

	private void whereFieldsSeparator(String conditionalPartOfQuery) {
		String relationalQueries[] = conditionalPartOfQuery.split("\\s+and\\s+|\\s+or\\s+");
		for (String relationQuery : relationalQueries) {
			WhereRestrictionalConditions restrictcond = new WhereRestrictionalConditions();
			restrictcond.setColumn(relationQuery.split("<=|>=|!=|=|<|>")[0].trim());
			restrictcond.setValue(relationQuery.split("<=|>=|!=|=|<|>")[1].trim());
			restrictcond.setOperator((relationQuery.split(restrictcond.getColumn())[1]).split(restrictcond.getValue())[0].trim());
			queryParameter.getRestrictions().add(restrictcond);
		}
		if (queryParameter.getRestrictions().size() > 1)
			this.logicalOperatorSeparator(conditionalPartOfQuery);
	}

	private void logicalOperatorSeparator(String conditionQuery) {
		String conditionQueryData[] = conditionQuery.split(" ");
		for (String queryData : conditionQueryData) {
			if (queryData.equals("and") || queryData.equals("or")) {
				queryParameter.getLogicalOperator().add(queryData);
			}
		}
		queryParameter.setLogicalOperator(queryParameter.getLogicalOperator());
	}

	private void fieldSeparation(String selectColumn) {
		if (selectColumn.trim().contains("*") && selectColumn.trim().length() == 1) {
			queryParameter.setHasAllColumn(true);
		} else {
			String columnList[] = selectColumn.trim().split(",");
			for (String column : columnList) {
				queryParameter.getColumNames().add(column.trim().toLowerCase());
			}
			if (selectColumn.contains("sum(") || selectColumn.contains("count(") || selectColumn.contains("count(*)")) {
				queryParameter.setHasAggregate(true);
				queryParameter.setHasAllColumn(true);
			}
		}
		queryParameter.setColumNames(queryParameter.getColumNames());
	}

	public LinkedHashMap<String, Integer> setHeaderRow() throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryParameter.getFilePath()));
		if (bufferedReader != null) {
			String rowData = bufferedReader.readLine();
			String rowValues[] = rowData.split(",");
			int columnIndex = 0;
			for (String rowvalue : rowValues) {
				queryParameter.getHeaderRow().put(rowvalue.toLowerCase(), columnIndex);
				columnIndex++;
			}
		}
		bufferedReader.close();
		return queryParameter.getHeaderRow();
	}
}