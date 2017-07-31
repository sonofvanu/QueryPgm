package com.stackroute.datamunging.model;

//change class name
public class WhereRestrictionalConditions {
	// change name to conditionColumn,conditionOperator,conditionValue
	private String columnName, operatorSymbol, conditionalValue;

	public String getColumn() {
		return columnName;
	}

	public void setColumn(String column) {
		this.columnName = column;
	}

	public String getOperator() {
		return operatorSymbol;
	}

	public void setOperator(String operator) {
		this.operatorSymbol = operator;
	}

	public String getValue() {
		return conditionalValue;
	}

	public void setValue(String value) {
		this.conditionalValue = value;
	}

	public String toString() {
		return "Column:" + columnName + " Operator:" + operatorSymbol + " Value:" + conditionalValue;
	}

}
