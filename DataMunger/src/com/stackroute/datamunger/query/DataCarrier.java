package com.stackroute.datamunger.query;
import java.util.*;

public class DataCarrier
{
	private List<RowDataHolder> resultSet=new ArrayList<RowDataHolder>();
	
	private LinkedHashMap<String,Float> aggregateRow=new LinkedHashMap<String,Float>();
	
	private LinkedHashMap<String,List<RowDataHolder>> groupByDataSetNew=new LinkedHashMap<String,List<RowDataHolder>>();
	
	private LinkedHashMap<String,LinkedHashMap<String,Float>> totalGroupedData=new LinkedHashMap<String,LinkedHashMap<String,Float>>(); 
	
	public LinkedHashMap<String,LinkedHashMap<String, Float>> getTotalGroupedData() 
	{
		return totalGroupedData;
	}

	public void setTotalGroupedData(LinkedHashMap<String,LinkedHashMap<String, Float>> totalGroupedData) 
	{
		this.totalGroupedData = totalGroupedData;
	}

	public LinkedHashMap<String, List<RowDataHolder>> getGroupByDataSetNew() 
	{
		return groupByDataSetNew;
	}

	public void setGroupByDataSetNew(LinkedHashMap<String, List<RowDataHolder>> groupByDataSetNew) 
	{
		this.groupByDataSetNew = groupByDataSetNew;
	}

	public LinkedHashMap<String, Float> getAggregateRow() {
		return aggregateRow;
	}

	public void setAggregateRow(LinkedHashMap<String, Float> aggregateRow) {
		this.aggregateRow = aggregateRow;
	}

	public List<RowDataHolder> getResultSet() 
	{
		return resultSet;
	}

	public void setResultSet(List<RowDataHolder> resultSet) 
	{
		this.resultSet = resultSet;
	}
	
	
}
