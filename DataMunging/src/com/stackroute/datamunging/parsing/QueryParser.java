package com.stackroute.datamunging.parsing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import com.stackroute.datamunging.model.WhereRestrictionalConditions;

public class QueryParser {

	QueryParameter queryParameter = new QueryParameter();

	@SuppressWarnings("unused")
	public QueryParser(String queryString) throws Exception {

		queryParameter.setColumNames(queryParameter.columNames);
		queryParameter.setListrelexpr(new ArrayList<WhereRestrictionalConditions>());
		queryParameter.setLogicalOperator(new ArrayList<String>());
		this.querySegregator(queryString);
		queryParameter.headerRow = this.setHeaderRow();
	}

	public QueryParameter querySegregator(String queryString) {
		String basePartofQuery = null, conditionalQueryPart = null, selectedColumnOfQuery = null;

		if (queryString.contains("order by")) {
			basePartofQuery = queryString.split("order by")[0].trim();
			queryParameter.setOrderByColumn(queryString.split("order by")[1].trim().toLowerCase());
			if (basePartofQuery.contains("where")) {
				conditionalQueryPart = basePartofQuery.split("where")[1].trim();
				this.relationalQueryFieldProcessor(conditionalQueryPart);
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
				conditionalQueryPart = basePartofQuery.split("where")[1].trim();
				this.relationalQueryFieldProcessor(conditionalQueryPart);
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
			conditionalQueryPart = queryString.split("where")[1];
			conditionalQueryPart = conditionalQueryPart.trim();
			queryParameter.setFilePath(basePartofQuery.split("from")[1].trim());
			basePartofQuery = basePartofQuery.split("from")[0].trim();
			this.relationalQueryFieldProcessor(conditionalQueryPart);
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

	private void relationalQueryFieldProcessor(String conditionQuery) {
		String oper[] = { ">=", "<=", ">", "<", "!=", "=" };

		String relationalQueries[] = conditionQuery.split("\\s+and\\s+|\\s+or\\s+");

		for (String relationQuery : relationalQueries) {
			relationQuery = relationQuery.trim();
			for (String operator : oper) {
				if (relationQuery.contains(operator)) {
					WhereRestrictionalConditions restrictcond = new WhereRestrictionalConditions();
					restrictcond.setColumn(relationQuery.split(operator)[0].trim());
					restrictcond.setValue(relationQuery.split(operator)[1].trim());
					restrictcond.setOperator(operator);
					queryParameter.restrictions.add(restrictcond);
					break;
				}
			}
		}

		queryParameter.setRestrictions(queryParameter.restrictions);
		if (queryParameter.restrictions.size() > 1)
			this.logicalOperatorManager(conditionQuery);
	}

	private void logicalOperatorManager(String conditionQuery) {
		String conditionQueryData[] = conditionQuery.split(" ");

		for (String queryData : conditionQueryData) {
			queryData = queryData.trim();
			if (queryData.equals("and") || queryData.equals("or")) {
				queryParameter.logicalOperator.add(queryData);
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
				queryParameter.columNames.add(column.trim().toLowerCase());
			}

			if (selectColumn.contains("sum(") || selectColumn.contains("count(") || selectColumn.contains("count(*)")) {
				queryParameter.setHasAggregate(true);
				queryParameter.setHasAllColumn(true);
			}
		}
	}

	public HashMap<String,Integer> setHeaderRow() throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryParameter.getFilePath()));
		//HeaderRowData headerRow = new HeaderRowData();

		if (bufferedReader != null) {
			String rowData = bufferedReader.readLine();
			String rowValues[] = rowData.split(",");
			int columnIndex = 0;
			for (String rowvalue : rowValues) {
				queryParameter.headerRow.put(rowvalue.toLowerCase(), columnIndex);
				columnIndex++;
			}
		}
		queryParameter.setHeaderRow(queryParameter.headerRow);

		bufferedReader.close();
		return queryParameter.headerRow;
	}

}
