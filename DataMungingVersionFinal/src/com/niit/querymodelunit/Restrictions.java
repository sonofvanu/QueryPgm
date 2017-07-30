package com.niit.querymodelunit;

public class Restrictions {
	private String columnName;
	private String operator;
	private String value;
	
	
	public void setColumnName(String col_name) {
		this.columnName = col_name;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getColumnName() {
		return columnName;
	}
	public String getOperator() {
		return operator;
	}
	public String getValue() {
		return value;
	}
	
	
}
