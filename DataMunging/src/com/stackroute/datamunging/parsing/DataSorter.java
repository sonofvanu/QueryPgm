package com.stackroute.datamunging.parsing;

import java.util.Comparator;
import com.stackroute.datamunging.model.RowDataHolder;

public class DataSorter implements Comparator<RowDataHolder> {
	private int sortingIndex;

	public int getSortingIndex() {
		return sortingIndex;
	}

	public void setSortingIndex(int sortingIndex) {
		this.sortingIndex = sortingIndex;
	}

	@Override
	public int compare(RowDataHolder arg0, RowDataHolder arg1) {
		return arg0.get(sortingIndex).compareTo(arg1.get(sortingIndex));
	}

}
