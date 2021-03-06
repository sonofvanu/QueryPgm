package com.stackroute.datamunger.query;

import java.util.Comparator;


public class DataSorter implements Comparator<RowDataHolder> 
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
	public int compare(RowDataHolder arg0, RowDataHolder arg1) 
	{
		return arg0.get(sortingIndex).compareTo(arg1.get(sortingIndex));
	}

}
