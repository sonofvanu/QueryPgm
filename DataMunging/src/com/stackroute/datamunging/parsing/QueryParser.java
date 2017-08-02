package com.stackroute.datamunging.parsing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.stackroute.datamunging.model.WhereRestrictionalConditions;

public class QueryParser {

	QueryParameter queryParameter = new QueryParameter();

	public QueryParameter querySegregator(String queryString) {
		String basePartofQuery = null, conditionalPartOfQuery = null, selectedColumnOfQuery = null;
		if (queryString.contains("order by")) {
			//select id, name from emp where salary>1000 order by department
			basePartofQuery = queryString.split("order by")[0].trim();
			queryParameter.setOrderByColumn(queryString.split("order by")[1].trim().toLowerCase());
			if (basePartofQuery.contains("where")) {
				conditionalPartOfQuery = basePartofQuery.split("where")[1].trim();
				this.whereFieldsSeparator(conditionalPartOfQuery);
				basePartofQuery = basePartofQuery.split("where")[0].trim();
				queryParameter.setHasWhere(true);
			}
			queryParameter.setFilePath(basePartofQuery.split("from")[1].trim());
			basePartofQuery = basePartofQuery.split("from")[0].trim();
			selectedColumnOfQuery = basePartofQuery.split("select")[1].trim();
			this.fieldSeparation(selectedColumnOfQuery);
			queryParameter.setHasOrderBy(true);
		} else if (queryString.contains("group by")) {
			basePartofQuery = queryString.split("group by")[0].trim();
			queryParameter.setGroupByColumn(queryString.split("group by")[1].trim().toLowerCase());
			if (basePartofQuery.contains("where")) {
				conditionalPartOfQuery = basePartofQuery.split("where")[1].trim();
				this.whereFieldsSeparator(conditionalPartOfQuery);
				basePartofQuery = basePartofQuery.split("where")[0].trim();
				queryParameter.setHasWhere(true);
			}
			queryParameter.setFilePath(basePartofQuery.split("from")[1].trim());
			basePartofQuery = basePartofQuery.split("from")[0].trim();
			selectedColumnOfQuery = basePartofQuery.split("select")[1].trim();
			this.fieldSeparation(selectedColumnOfQuery);
			queryParameter.setHasGroupBy(true);
		} else if (queryString.contains("where")) {
			basePartofQuery = queryString.split("where")[0];
			conditionalPartOfQuery = queryString.split("where")[1];
			conditionalPartOfQuery = conditionalPartOfQuery.trim();
			queryParameter.setFilePath(basePartofQuery.split("from")[1].trim());
			basePartofQuery = basePartofQuery.split("from")[0].trim();
			this.whereFieldsSeparator(conditionalPartOfQuery);
			selectedColumnOfQuery = basePartofQuery.split("select")[1].trim();
			this.fieldSeparation(selectedColumnOfQuery);
			queryParameter.setHasWhere(true);
		} else {
			basePartofQuery = queryString.split("from")[0].trim();
			queryParameter.setFilePath(queryString.split("from")[1].trim());
			selectedColumnOfQuery = basePartofQuery.split("select")[1].trim();
			this.fieldSeparation(selectedColumnOfQuery);
			queryParameter.setHasSimpleQuery(true);
		}
		
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
		for (String queryData : conditionQueryData) {// no need to use loop
			if (queryData.equals("and") || queryData.equals("or")) {
				queryParameter.getLogicalOperator().add(queryData);//this logic will not work properly
				
			}
		}
		queryParameter.setLogicalOperator(queryParameter.logicalOperator);
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
	
	public QueryParser(String queryString) throws Exception {
		queryParameter.setColumNames(queryParameter.getColumNames()); //should not be public properties
		queryParameter.setListrelexpr(new ArrayList<WhereRestrictionalConditions>()); // this will set empty object
		queryParameter.setLogicalOperator(new ArrayList<String>());
		this.querySegregator(queryString);
		queryParameter.setHeaderRow(this.setHeaderRow());////should not be public properties
	}
}