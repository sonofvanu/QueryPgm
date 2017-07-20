package com.niit.queryprogression;

public class Aggregators {
	private String aggreFunc;
	private String column;
	private int result;

	public String getAggreFunc() {
		return aggreFunc;
	}

	public void setAggreFunc(String aggreFunc) {
		this.aggreFunc = aggreFunc;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public int getResult() {
		return result;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public String toString() {
		return getAggreFunc() + "(" + getColumn() + ")" + getResult();
	}
}
