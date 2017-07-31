package com.stackroute.datamunging.parsing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import com.stackroute.datamunging.model.HeaderRowData;
import com.stackroute.datamunging.model.SpecifiedColumns;
import com.stackroute.datamunging.model.WhereRestrictionalConditions;

public class QueryParser {

	QueryParameter queryParameter = new QueryParameter();

	public QueryParser(String queryString) throws Exception {

		queryParameter.setColumNames(new SpecifiedColumns());
		queryParameter.setListrelexpr(new ArrayList<WhereRestrictionalConditions>());
		queryParameter.setLogicalOperator(new ArrayList<String>());
		this.parseQuery(queryString);
		HeaderRowData headerRow = this.setHeaderRow();
	}

	public QueryParameter parseQuery(String queryString) {
		String baseQuery = null, conditionQuery = null, selectcol = null;

		if (queryString.contains("order by")) {
			baseQuery = queryString.split("order by")[0].trim();
			queryParameter.setOrderByColumn(queryString.split("order by")[1].trim().toLowerCase());
			if (baseQuery.contains("where")) {
				conditionQuery = baseQuery.split("where")[1].trim();
				this.relationalExpressionProcessing(conditionQuery);
				baseQuery = baseQuery.split("where")[0].trim();
				queryParameter.setHasWhere(true);
			}
			queryParameter.setFilePath(baseQuery.split("from")[1].trim());
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			queryParameter.setHasOrderBy(true);

		} else if (queryString.contains("group by")) {
			baseQuery = queryString.split("group by")[0].trim();
			queryParameter.setGroupByColumn(queryString.split("group by")[1].trim().toLowerCase());
			if (baseQuery.contains("where")) {
				conditionQuery = baseQuery.split("where")[1].trim();
				this.relationalExpressionProcessing(conditionQuery);
				baseQuery = baseQuery.split("where")[0].trim();
				queryParameter.setHasWhere(true);
			}
			queryParameter.setFilePath(baseQuery.split("from")[1].trim());
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			queryParameter.setHasGroupBy(true);
		} else if (queryString.contains("where")) {
			baseQuery = queryString.split("where")[0];
			conditionQuery = queryString.split("where")[1];
			conditionQuery = conditionQuery.trim();
			queryParameter.setFilePath(baseQuery.split("from")[1].trim());
			baseQuery = baseQuery.split("from")[0].trim();
			this.relationalExpressionProcessing(conditionQuery);
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			queryParameter.setHasWhere(true);

		} else {
			baseQuery = queryString.split("from")[0].trim();
			queryParameter.setFilePath(queryString.split("from")[1].trim());
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			queryParameter.setHasSimpleQuery(true);

		}

		return queryParameter;
	}

	private void relationalExpressionProcessing(String conditionQuery) {
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
			this.logicalOperatorStore(conditionQuery);
	}

	private void logicalOperatorStore(String conditionQuery) {
		String conditionQueryData[] = conditionQuery.split(" ");

		for (String queryData : conditionQueryData) {
			queryData = queryData.trim();
			if (queryData.equals("and") || queryData.equals("or")) {
				queryParameter.logicalOperator.add(queryData);
			}
		}
		queryParameter.setLogicalOperator(queryParameter.logicalOperator);
	}

	private void fieldsProcessing(String selectColumn) {

		if (selectColumn.trim().contains("*") && selectColumn.trim().length() == 1) {
			queryParameter.setHasAllColumn(true);
		} else {
			String columnList[] = selectColumn.trim().split(",");

			for (String column : columnList) {
				queryParameter.columNames.getColumns().add(column.trim().toLowerCase());
			}

			if (selectColumn.contains("sum(") || selectColumn.contains("count(") || selectColumn.contains("count(*)")) {
				queryParameter.setHasAggregate(true);
				queryParameter.setHasAllColumn(true);
			}
		}
	}

	public HeaderRowData setHeaderRow() throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(queryParameter.getFilePath()));
		HeaderRowData headerRow = new HeaderRowData();

		if (bufferedReader != null) {
			String rowData = bufferedReader.readLine();
			String rowValues[] = rowData.split(",");
			int columnIndex = 0;
			for (String rowvalue : rowValues) {
				headerRow.put(rowvalue.toLowerCase(), columnIndex);
				columnIndex++;
			}
		}
		queryParameter.setHeaderRow(headerRow);

		return headerRow;
	}

}
