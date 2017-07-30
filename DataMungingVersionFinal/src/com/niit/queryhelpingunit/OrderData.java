package com.niit.queryhelpingunit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.niit.queryparsingunit.QueryParser;

public class OrderData {
	public Map<Integer,String> resultantDataOrdering(QueryParser queryParser,Map<Integer,String> dataset,Map<String,Integer> header){
		BufferedReader bufferedReader;
		Map<Integer, String> rowData = new HashMap<>();
		try {
			bufferedReader = new BufferedReader(new FileReader(queryParser.getFileLocation()));

			int startPoint = 0;
			String string = bufferedReader.readLine();
			string = bufferedReader.readLine();
			List<String> orderByColumnList = new ArrayList<>();
			String afterSplitting[]=null;
			while (string != null) {
				afterSplitting = string.split(",");
				orderByColumnList.add(afterSplitting[header.get(queryParser.getOrderByClause())]);
				string = bufferedReader.readLine();
			}

			Collections.sort(orderByColumnList);
			bufferedReader.close();

			int lengthOfOrderByColumnList = orderByColumnList.size();

			
			for (int k = 0; k < lengthOfOrderByColumnList; k++) {
				try{
				for(Map.Entry<Integer,String> entry: dataset.entrySet()){
					if(entry.getValue().contains(orderByColumnList.get(k))){
						rowData.put(startPoint, entry.getValue());
						startPoint++;
						dataset.remove(entry.getKey());
					}
				}
				}
				catch(ConcurrentModificationException cee){}
			}
		}

		catch (IOException io) {

		}

		return rowData;
	}

}
