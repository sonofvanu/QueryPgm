package com.stackroute.datamunging.model;

public class WhereRestrictionalConditions {
	private String conditionalColumn, conditionalOperator, conditionalValue;

	public String getColumn() {
		return conditionalColumn;
	}

	public void setColumn(String column) {
		this.conditionalColumn = column;
	}

	public String getOperator() {
		return conditionalOperator;
	}

	public void setOperator(String operator) {
		this.conditionalOperator = operator;
	}

	public String getValue() {
		return conditionalValue;
	}

	public void setValue(String value) {
		this.conditionalValue = value;
	}

}
