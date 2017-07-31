package com.stackroute.datamunging.model;

import java.util.*;

public class SpecifiedColumns {
	private List<String> columnNameList = new ArrayList<String>();

	public List<String> getColumns() {
		return columnNameList;
	}

	public void setColumns(List<String> columns) {
		this.columnNameList = columns;
	}

}
