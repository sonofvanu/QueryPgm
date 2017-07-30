package com.niit.querymodelunit;

public class Aggregate {
	private String aggregateFunction;
	private String column;
	private int result;
	
	
	
	public void setAggregateFunction(String aggreFunc) {
		this.aggregateFunction = aggreFunc;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public void setResult(int result) {
		this.result = result;
	}
	public String getAggregateFunction() {
		return aggregateFunction;
	}
	public String getColumn() {
		return column;
	}
	public int getResult() {
		return result;
	}
	public String toString(){
		return getAggregateFunction()+"("+getColumn()+")"+getResult();
	}
	
	
}
