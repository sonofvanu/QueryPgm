package com.stackroute.datamunging.parsing;

import java.util.List;

import com.stackroute.datamunging.model.HeaderRowData;
import com.stackroute.datamunging.model.SpecifiedColumns;
import com.stackroute.datamunging.model.WhereRestrictionalConditions;

public class QueryParameter {
	private String filePath;
	private String orderByColumn,groupByColumn;
	public List<WhereRestrictionalConditions> getRestrictions() {
		return restrictions;
	}

	public void setRestrictions(List<WhereRestrictionalConditions> restrictions) {
		this.restrictions = restrictions;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public void setOrderByColumn(String orderByColumn) {
		this.orderByColumn = orderByColumn;
	}

	public void setGroupByColumn(String groupByColumn) {
		this.groupByColumn = groupByColumn;
	}

	public void setHasGroupBy(boolean hasGroupBy) {
		this.hasGroupBy = hasGroupBy;
	}

	public void setHasOrderBy(boolean hasOrderBy) {
		this.hasOrderBy = hasOrderBy;
	}

	public void setHasWhere(boolean hasWhere) {
		this.hasWhere = hasWhere;
	}

	public void setHasAllColumn(boolean hasAllColumn) {
		this.hasAllColumn = hasAllColumn;
	}

	public void setHasColumn(boolean hasColumn) {
		this.hasColumn = hasColumn;
	}

	public void setHasSimpleQuery(boolean hasSimpleQuery) {
		this.hasSimpleQuery = hasSimpleQuery;
	}

	public void setHasAggregate(boolean hasAggregate) {
		this.hasAggregate = hasAggregate;
	}

	public void setHeaderRow(HeaderRowData headerRow) {
		this.headerRow = headerRow;
	}

	SpecifiedColumns columNames;
	 List<WhereRestrictionalConditions> restrictions;
	private boolean hasGroupBy=false,hasOrderBy=false,hasWhere=false,hasAllColumn=false,hasColumn=false,hasSimpleQuery,hasAggregate=false;
 List<String> logicalOperator;
	public void setLogicalOperator(List<String> logicalOperator) {
		this.logicalOperator = logicalOperator;
	}

	private HeaderRowData headerRow;
	
	
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
	
	public HeaderRowData getHeaderRow() 
	{
		return headerRow;
	}

	public List<String> getLogicalOperator() {
		return logicalOperator;
	}

	public SpecifiedColumns getColumNames() {
		return columNames;
	}

	public void setColumNames(SpecifiedColumns columnNames) {
		this.columNames = columnNames;
	}

	public List<WhereRestrictionalConditions> getListrelexpr() {
		return restrictions;
	}

	public void setListrelexpr(List<WhereRestrictionalConditions> listrelexpr) {
		this.restrictions = listrelexpr;
	}

	public String getOrderByColumn() 
	{
		return orderByColumn;
	}
}
