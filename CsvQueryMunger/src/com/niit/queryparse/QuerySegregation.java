package com.niit.queryparse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.niit.model.Restrictions;
import com.niit.queryoperation.Columisation;
import com.niit.queryoperation.HeaderData;

public class QuerySegregation {

	private String filePath;
	private String orderByColumn, groupByColumn;
	private Columisation columNames;
	private List<Restrictions> restrictions;
	private boolean hasGroupBy = false, hasOrderBy = false, hasWhere = false, hasAllColumn = false, hasColumn = false,
			hasSimpleQuery, hasAggregate = false;
	private List<String> logicalOperator;
	private HeaderData headerRow;

	public String getFilePath() {
		return filePath;
	}

	public String getGroupByColumn() {
		return groupByColumn;
	}

	public boolean isHasGroupBy() {
		return hasGroupBy;
	}

	public boolean isHasOrderBy() {
		return hasOrderBy;
	}

	public boolean isHasWhere() {
		return hasWhere;
	}

	public boolean isHasAllColumn() {
		return hasAllColumn;
	}

	public boolean isHasColumn() {
		return hasColumn;
	}

	public boolean isHasSimpleQuery() {
		return hasSimpleQuery;
	}

	public boolean isHasAggregate() {
		return hasAggregate;
	}

	public HeaderData getHeaderRow() {
		return headerRow;
	}

	public List<String> getLogicalOperator() {
		return logicalOperator;
	}

	public Columisation getColumNames() {
		return columNames;
	}

	public void setColumNames(Columisation columnNames) {
		this.columNames = columnNames;
	}

	public List<Restrictions> getListrelexpr() {
		return restrictions;
	}

	public void setListrelexpr(List<Restrictions> listrelexpr) {
		this.restrictions = listrelexpr;
	}

	public String getOrderByColumn() {
		return orderByColumn;
	}

	public QuerySegregation(String queryString) throws Exception {
		columNames = new Columisation();
		restrictions = new ArrayList();
		logicalOperator = new ArrayList<String>();
		this.parseQuery(queryString);
		headerRow = this.csvHeaderRowDataFetching();
	}

	public QuerySegregation parseQuery(String queryString) {
		String baseQuery = null, conditionQuery = null, selectcol = null;

		if (queryString.contains("order by")) {
			baseQuery = queryString.split("order by")[0].trim();
			orderByColumn = queryString.split("order by")[1].trim();
			if (baseQuery.contains("where")) {
				conditionQuery = baseQuery.split("where")[1].trim();
				this.conditionalExpressionParsing(conditionQuery);
				baseQuery = baseQuery.split("where")[0].trim();
				hasWhere = true;
			}
			filePath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.conditionalFieldsSeparator(selectcol);
			hasOrderBy = true;
		} else if (queryString.contains("group by")) {
			baseQuery = queryString.split("group by")[0].trim();
			groupByColumn = queryString.split("group by")[1].trim();
			if (baseQuery.contains("where")) {
				conditionQuery = baseQuery.split("where")[1].trim();
				this.conditionalExpressionParsing(conditionQuery);
				baseQuery = baseQuery.split("where")[0].trim();
				hasWhere = true;
			}
			filePath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.conditionalFieldsSeparator(selectcol);
			hasGroupBy = true;
		} else if (queryString.contains("where")) {
			baseQuery = queryString.split("where")[0];
			conditionQuery = queryString.split("where")[1];
			conditionQuery = conditionQuery.trim();
			filePath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			this.conditionalExpressionParsing(conditionQuery);
			selectcol = baseQuery.split("select")[1].trim();
			this.conditionalFieldsSeparator(selectcol);
			hasWhere = true;
		} else {
			baseQuery = queryString.split("from")[0].trim();
			filePath = queryString.split("from")[1].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.conditionalFieldsSeparator(selectcol);
			hasSimpleQuery = true;
		}

		return this;
	}

	private void conditionalExpressionParsing(String conditionQuery) {
		String oper[] = { ">=", "<=", ">", "<", "!=", "=" };

		String relationalQueries[] = conditionQuery.split("and|or");

		for (String relationQuery : relationalQueries) {
			relationQuery = relationQuery.trim();
			for (String operator : oper) {
				if (relationQuery.contains(operator)) {
					Restrictions criteria = new Restrictions();
					criteria.setColumn(relationQuery.split(operator)[0].trim());
					criteria.setValue(relationQuery.split(operator)[1].trim());
					criteria.setOperator(operator);
					restrictions.add(criteria);
					break;
				}
			}
		}

		if (restrictions.size() > 1)
			this.whereLogicalOperatorData(conditionQuery);
	}

	private void whereLogicalOperatorData(String conditionQuery) {
		String conditionQueryData[] = conditionQuery.split(" ");

		for (String queryData : conditionQueryData) {
			queryData = queryData.trim();
			if (queryData.equals("and") || queryData.equals("or")) {
				logicalOperator.add(queryData);
			}
		}
	}

	private void conditionalFieldsSeparator(String selectColumn) {
		if (selectColumn.trim().contains("*") && selectColumn.trim().length() == 1) {
			hasAllColumn = true;
		} else {
			String columnList[] = selectColumn.trim().split(",");

			for (String column : columnList) {
				columNames.getColumns().add(column.trim());
			}

			if (selectColumn.contains("sum") || selectColumn.contains("count") || selectColumn.contains("count(*)")) {
				hasAggregate = true;
			}
		}
	}

	public HeaderData csvHeaderRowDataFetching() throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(getFilePath()));
		HeaderData headerRow = new HeaderData();

		if (bufferedReader != null) {
			String rowData = bufferedReader.readLine();
			String rowValues[] = rowData.split(",");
			int columnIndex = 0;
			for (String rowvalue : rowValues) {
				headerRow.put(rowvalue, columnIndex);
				columnIndex++;
			}
		}

		return headerRow;
	}

}
