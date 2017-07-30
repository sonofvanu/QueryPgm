package com.niit.model;

public class Restrictions {
	private String column,operator,value;
	
	public String getColumn() 
	{
		return column;
	}

	public void setColumn(String column) 
	{
		this.column = column;
	}

	public String getOperator() 
	{
		return operator;
	}

	public void setOperator(String operator) 
	{
		this.operator = operator;
	}

	public String getValue() 
	{
		return value;
	}

	public void setValue(String value) 
	{
		this.value = value;
	}
	
	public String toString()
	{
		return "Column:"+column+" Operator:"+operator+" Value:"+value;
	}

}
