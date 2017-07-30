package com.niit.processor;

import java.util.Comparator;

import com.niit.queryoperation.RowData;


public class DataSorting implements Comparator<RowData> 
{
	private int sortingIndex;
	
	public int getSortingIndex() 
	{
		return sortingIndex;
	}

	public void setSortingIndex(int sortingIndex) 
	{
		this.sortingIndex = sortingIndex;
	}

	@Override
	public int compare(RowData arg0, RowData arg1) 
	{
		return arg0.get(sortingIndex).compareTo(arg1.get(sortingIndex));
	}

}
