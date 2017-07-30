package com.niit.queryparsingunit;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.niit.querymodelunit.Restrictions;

public class QueryParser implements Cloneable {
	private List<String> columnNameList = new ArrayList<>();
	private String fileLocation;
	private boolean isWhere,isOrderBy,isGroupBy,hasAggregate;
	private List<Restrictions> consitionsList = new ArrayList<>();
	private List<String> logicalOperatorList = new ArrayList<>();
	private String groupByCondition,orderByClause,queryFormat;
	
	
	
	public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	public boolean isGroupBy() {
		return isGroupBy;
	}
	public boolean isOrderBy() {
		return isOrderBy;
	}
	public boolean isWhere() {
		return isWhere;
	}
	public String getQueryFormat() {
		return queryFormat;
	}
	public List<String> getColumnNameList() {
		return columnNameList;
	}
	public String getFileLocation() {
		return fileLocation;
	}
	public boolean isHasAggregate() {
		return hasAggregate;
	}
	public List<Restrictions> getConsitionsList() {
		return consitionsList;
	}
	public List<String> getLogicalOperatorList() {
		return logicalOperatorList;
	}
	public String getGroupByCondition() {
		return groupByCondition;
	}
	public String getOrderByClause() {
		return orderByClause;
	}
	
	private void checkaggregate() {

		for (String columns : columnNameList) {
			if (columns.contains("sum") || columns.contains("count") || columns.contains("avg") || columns.contains("min")
					|| columns.contains("max")) {
				queryFormat="aggregate";
				hasAggregate = true;
			} else
				hasAggregate = false;

		}
	}
	
	public QueryParser querySplitting(String query){
		
		String queryContainer = query;
		String splittedQuery[];

		Pattern pattern = Pattern.compile("select\\s.*from\\s.*");
		Matcher matcher = pattern.matcher(queryContainer);
		if (matcher.find()) {
			if (queryContainer.contains("group by")) {
				isGroupBy=true;
				splittedQuery = queryContainer.split("group by");
				groupByCondition = splittedQuery[1].replaceAll("\\s", "");
				queryContainer = splittedQuery[0];

			}
			if (queryContainer.contains("order by")) {
				isOrderBy=true;
				splittedQuery = queryContainer.split("order by");
				orderByClause = splittedQuery[1].replaceAll("\\s", "");
				queryContainer = splittedQuery[0];

			}
			if (queryContainer.contains("where")) {
				List<String> listOfOperandsInQuery = new ArrayList<>();
				queryFormat="simple";
				isWhere=true;
				String fullWhereCondition = "";
				splittedQuery = queryContainer.split("where");
				fullWhereCondition = splittedQuery[1];

				String whereConditionHolder = fullWhereCondition;
				StringBuilder stringBuilder = new StringBuilder();
				for (String string : whereConditionHolder.split(" ")) {

					if (!string.equals("")) // ignore space
						stringBuilder.append(string + " "); // add word with 1 space

				}
				whereConditionHolder = stringBuilder.toString();
				stringBuilder.setLength(0);
				String afterSplittingContainer[] = whereConditionHolder.split(" ");
				int afterSplittingLength = afterSplittingContainer.length;
				String newSplittedQuery[] = new String[afterSplittingLength + 1];
				for (int s = 0; s < afterSplittingLength; s++) {
					newSplittedQuery[s] = afterSplittingContainer[s];
				}
				newSplittedQuery[afterSplittingLength] = "dummy";

				for (String newlySplitted : newSplittedQuery) {
					if (!newlySplitted.equals("and") && !newlySplitted.equals("or") && !newlySplitted.equals("dummy")) {
						stringBuilder.append(newlySplitted);
					} else if (newlySplitted.equals("and")) {
						logicalOperatorList.add("and");
						listOfOperandsInQuery.add(stringBuilder.toString());
						stringBuilder.setLength(0);
					} else if (newlySplitted.equals("or")) {
						logicalOperatorList.add("or");
						listOfOperandsInQuery.add(stringBuilder.toString());
						stringBuilder.setLength(0);
					} else if (newlySplitted.equals("dummy")) {
						listOfOperandsInQuery.add(stringBuilder.toString());
						stringBuilder.setLength(0);
					}

				}

				int operandsLength = listOfOperandsInQuery.size();
				Restrictions restriction[] = new Restrictions[operandsLength];
				for (int valueAssigning = 0; valueAssigning < operandsLength; valueAssigning++) {
					restriction[valueAssigning] = new Restrictions();
					String splittedConditions[] = listOfOperandsInQuery.get(valueAssigning).split("[\\s]*[>=|<=|!=|=|<|>][\\s]*");
					int conditionsSplitLength = splittedConditions.length;

					if (conditionsSplitLength == 3) {
						restriction[valueAssigning].setColumnName(splittedConditions[0]);
						restriction[valueAssigning].setValue(splittedConditions[2]);
						if (listOfOperandsInQuery.get(valueAssigning).contains(">="))
							restriction[valueAssigning].setOperator(">=");
						if (listOfOperandsInQuery.get(valueAssigning).contains("<="))
							restriction[valueAssigning].setOperator("<=");
						if (listOfOperandsInQuery.get(valueAssigning).contains("!="))
							restriction[valueAssigning].setOperator("!=");
					} else if (conditionsSplitLength == 2) {
						restriction[valueAssigning].setColumnName(splittedConditions[0]);
						restriction[valueAssigning].setValue(splittedConditions[1]);
						if (listOfOperandsInQuery.get(valueAssigning).contains(">"))
							restriction[valueAssigning].setOperator(">");
						if (listOfOperandsInQuery.get(valueAssigning).contains("<"))
							restriction[valueAssigning].setOperator("<");
						if (listOfOperandsInQuery.get(valueAssigning).contains("="))
							restriction[valueAssigning].setOperator("=");
					}
					consitionsList.add(restriction[valueAssigning]);

				}
				queryContainer = splittedQuery[0];
			}
			if (queryContainer.contains("from")) {
				queryFormat="simple";
				splittedQuery = queryContainer.split("from");
				fileLocation = "G:\\" + splittedQuery[1].replaceAll("\\s", "");
				queryContainer = splittedQuery[0];
			}
			if (queryContainer.contains("select")) {
				splittedQuery = queryContainer.split("select");
				String columns = splittedQuery[1].replaceAll("\\s", "");
				if (!columns.equals("*")) {
					String columnNamesAfterSplitting[] = columns.trim().split(",");
					for (int c = 0; c < columnNamesAfterSplitting.length; c++) {
						columnNameList.add(columnNamesAfterSplitting[c]);
					}

					checkaggregate();
				} else {
					columnNameList.add("*");
				}
				queryContainer = splittedQuery[0];
				
			}
if(isGroupBy){
	queryFormat="groupby";
}
		}

		return this;
	}
	
	
}
