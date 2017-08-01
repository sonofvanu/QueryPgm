package com.stackroute.datamunging.model;

import java.util.*;

public class DataCarrier {
	// dataset for the resultant set of datas
	private List<RowDataHolder> resultSet = new ArrayList<RowDataHolder>();
	// dataset for the storage of aggregate functions
	private HashMap<String, Float> aggregateRow = new HashMap<String, Float>();
	// dataset for group by data
	private HashMap<String, List<RowDataHolder>> groupByDataSetNew = new HashMap<String, List<RowDataHolder>>();
	// dataset for group by with presence of aggregate functions
	private HashMap<String, HashMap<String, Float>> totalGroupedData = new HashMap<String, HashMap<String, Float>>();

	public HashMap<String, HashMap<String, Float>> getTotalGroupedData() {
		return totalGroupedData;
	}

	public void setTotalGroupedData(HashMap<String, HashMap<String, Float>> totalGroupedData) {
		this.totalGroupedData = totalGroupedData;
	}

	public HashMap<String, List<RowDataHolder>> getGroupByDataSetNew() {
		return groupByDataSetNew;
	}

	public void setGroupByDataSetNew(HashMap<String, List<RowDataHolder>> groupByDataSetNew) {
		this.groupByDataSetNew = groupByDataSetNew;
	}

	public HashMap<String, Float> getAggregateRow() {
		return aggregateRow;
	}

	public void setAggregateRow(HashMap<String, Float> aggregateRow) {
		this.aggregateRow = aggregateRow;
	}

	public List<RowDataHolder> getResultSet() {
		return resultSet;
	}

	public void setResultSet(List<RowDataHolder> resultSet) {
		this.resultSet = resultSet;
	}

}
