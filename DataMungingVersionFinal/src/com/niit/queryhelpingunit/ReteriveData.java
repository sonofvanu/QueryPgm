package com.niit.queryhelpingunit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.niit.queryparsingunit.QueryParser;

public class ReteriveData {
	
	
	public Map<Integer,String> csvFileReading(QueryParser queryParser, Map<String, Integer> headerData){
		Map<Integer, String> rowData = new HashMap<>();
		BufferedReader bufferedReader = null;
		int startingPoint = 0, helpingPoint = 0;
		int countOfColumn = 0;
		String string = "";
		String afterSplitting[];
		if(queryParser.getColumnNameList().get(0).equals("*")){
			if(queryParser.isWhere()){
				try {
					bufferedReader = new BufferedReader(new FileReader(queryParser.getFileLocation()));
					countOfColumn = queryParser.getColumnNameList().size();
					string = bufferedReader.readLine();
					string = bufferedReader.readLine();
					
					
					while (string != null) {
						afterSplitting = string.split(",");
						
				if (checkingRestrictionsConditions(afterSplitting, queryParser, headerData)) {
					rowData.put(startingPoint, string);	
				}
				startingPoint++;
				string = bufferedReader.readLine();

				}

				}
				catch(IOException io)
				{
					
				}

			}
			else{
				try {
					bufferedReader = new BufferedReader(new FileReader(queryParser.getFileLocation()));
					countOfColumn = queryParser.getColumnNameList().size();
					string = bufferedReader.readLine();
					string = bufferedReader.readLine();
					
					while (string != null) {
						rowData.put(startingPoint, string);

						startingPoint++;
						string = bufferedReader.readLine();

					}
					}
				catch(IOException io){}
			}
			
		}
		else{
			if(queryParser.isWhere()){
				try {
					bufferedReader = new BufferedReader(new FileReader(queryParser.getFileLocation()));
					countOfColumn = queryParser.getColumnNameList().size();
					string = bufferedReader.readLine();
					string = bufferedReader.readLine();
					while (string != null) {
						afterSplitting = string.split(",");
						
				if (checkingRestrictionsConditions(afterSplitting, queryParser, headerData)) {
					StringBuffer stringBuffer = new StringBuffer();
					for (helpingPoint = 0; helpingPoint < countOfColumn; helpingPoint++) {
						stringBuffer.append(afterSplitting[headerData.get(queryParser.getColumnNameList().get(helpingPoint))] + ",");
						String stringPlain = stringBuffer.toString();
						rowData.put(startingPoint, stringPlain.substring(0, stringPlain.length() - 1));
					}
				}
				startingPoint++;
				string = bufferedReader.readLine();

				}

				}
				catch(IOException io){}
		}
			else{
				try {
					bufferedReader = new BufferedReader(new FileReader(queryParser.getFileLocation()));
					countOfColumn = queryParser.getColumnNameList().size();
					string = bufferedReader.readLine();
					string = bufferedReader.readLine();
					
					while (string != null) {
						afterSplitting = string.split(",");
						StringBuffer stringBuffer = new StringBuffer();
						for (helpingPoint = 0; helpingPoint < countOfColumn; helpingPoint++) {
							stringBuffer.append(afterSplitting[headerData.get(queryParser.getColumnNameList().get(helpingPoint))] + ",");
							String stringPlain = stringBuffer.toString();
							rowData.put(startingPoint, stringPlain.substring(0, stringPlain.length() - 1));
						}
						startingPoint++;
						string = bufferedReader.readLine();
					}
				}
				catch(IOException io){}
			}
		}
		return rowData;
	}
	
	private boolean checkingRestrictionsConditions(String afterDataSplitting[],QueryParser queryParser,Map<String,Integer> headerData){
		int logicalOperatorLength = queryParser.getLogicalOperatorList().size() + 1;
		boolean manyRestrictions[] = new boolean[logicalOperatorLength];
		for (int indexPoint = 0; indexPoint < logicalOperatorLength; indexPoint++) {
			if (queryParser.getConsitionsList().get(indexPoint).getOperator().equals("=")) {
				manyRestrictions[indexPoint] = afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())].equals(queryParser.getConsitionsList().get(indexPoint).getValue());
			} else if (queryParser.getConsitionsList().get(indexPoint).getOperator().equals("!=")) {
				manyRestrictions[indexPoint] = !(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())].equals(queryParser.getConsitionsList().get(indexPoint).getValue()));
			} else if (queryParser.getConsitionsList().get(indexPoint).getOperator().equals(">")) {
				long restrict = Math.max(Long.parseLong(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())]),Long.parseLong(queryParser.getConsitionsList().get(indexPoint).getValue()));
				manyRestrictions[indexPoint] = Long.toString(restrict).equals(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())])&& !Long.toString(restrict).equals(queryParser.getConsitionsList().get(indexPoint).getValue());
			} else if (queryParser.getConsitionsList().get(indexPoint).getOperator().equals("<")) {
				long restrict = Math.min(Long.parseLong(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())]),Long.parseLong(queryParser.getConsitionsList().get(indexPoint).getValue()));
				manyRestrictions[indexPoint] = Long.toString(restrict).equals(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())])&& !Long.toString(restrict).equals(queryParser.getConsitionsList().get(indexPoint).getValue());

			} else if (queryParser.getConsitionsList().get(indexPoint).getOperator().equals(">=")) {
				long restrict = Math.max(Long.parseLong(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())]),Long.parseLong(queryParser.getConsitionsList().get(indexPoint).getValue()));
				manyRestrictions[indexPoint] = Long.toString(restrict).equals(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())]);
			} else if (queryParser.getConsitionsList().get(indexPoint).getOperator().equals("<=")) {
				long restrict = Math.min(Long.parseLong(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())]),Long.parseLong(queryParser.getConsitionsList().get(indexPoint).getValue()));
				manyRestrictions[indexPoint] = Long.toString(restrict).equals(afterDataSplitting[headerData.get(queryParser.getConsitionsList().get(indexPoint).getColumnName())]);

			}
		}
		boolean restriction = manyRestrictions[0];
		for (int startPoint = 0; startPoint < logicalOperatorLength - 1; startPoint++) {

			if (queryParser.getLogicalOperatorList().get(startPoint).equals("and")) {
				restriction = restriction && manyRestrictions[startPoint + 1];
			} else if (queryParser.getLogicalOperatorList().get(startPoint).equals("or")) {
				restriction = restriction || manyRestrictions[startPoint + 1];
			}
		}
		return restriction;
	}
	
	
}