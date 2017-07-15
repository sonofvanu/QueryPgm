package Query.QueryProctor;

import java.util.ArrayList;
import java.util.List;

public class QueryParser {

	public static String filepath, queryfields, querycondition, querryarray[],relationalquery;
	public String orderbycol, groupbycol, column;
	QueryInput queryinput = new QueryInput();
	public String query = queryinput.queryGetter();
	private RelationalConditions relationalcondition = new RelationalConditions();
	private List<RelationalConditions> relationalList;
	private boolean hasgroupby, hasorderby, haswhere, hasAllColumn, hasnormal, hasAggregate;
	private List<String> logicalOperator = new ArrayList<String>();
	public List<String> columnnamelist=new ArrayList<>();
	
	public List<String> getColumnnamelist() {
		return columnnamelist;
	}

	public void setColumnnamelist(List<String> columnnamelist) {
		this.columnnamelist = columnnamelist;
	}

	public ColumnNames columnames = new ColumnNames();

	public String selectcol;

	public String getSelectcol() {
		return selectcol;
	}

	public void setSelectcol(String selectcol) {
		this.selectcol = selectcol;
	}

	public String[] inputQuerryArray() {

		querryarray = query.split(" ");
		this.eligibleQuery(query);
		column = querryarray[1];
		System.out.println(column);
		return querryarray;
	}

	public boolean eligibleQuery(String query) {
		if ((query.contains("select") && query.contains("from")) || (query.contains("*") || query.contains("where")
				|| query.contains("group by") || query.contains("order by") || query.contains("sort by"))) {

			this.fieldsSeparator(query);
			return true;
		} else {
			System.out.println("You have entered an invalid query");
			return false;
		}

	}

	public QueryParser fieldsProcessing(String selectcolumn) {
		if (selectcolumn.trim().contains("*") && selectcolumn.length() == 1) {
			hasAllColumn = true;
		}
		if (selectcolumn.trim().contains(",")) {
			String columnlist[] = selectcolumn.split(",");

			int i = 0;
			for (String column : columnlist) {
				columnames.colnames.put(column, i);
				this.columnnamelist.add(column);
				i++;
			}

			if (selectcolumn.contains("sum") || selectcolumn.contains("count") || selectcolumn.contains("count(*)")) {
				hasAggregate = true;
			}
			// System.out.println(columnames.colnames);

		}
		return this;
	}

	public QueryParser fieldsSeparator(String query) {
		String baseQuery = null, conditionQuery = null, selectcol = null;

		if (query.contains("order by")) {
			baseQuery = query.split("order by")[0].trim();
			orderbycol = query.split("order by")[1].trim();
			filepath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasorderby = true;
		}
		if (query.contains("group by")) {
			baseQuery = query.split("group by")[0].trim();
			groupbycol = query.split("group by")[1].trim();
			if (baseQuery.contains("where")) {
				conditionQuery = baseQuery.split("where")[1].trim();
				this.relationalExpressionProcessing(conditionQuery);
				baseQuery = baseQuery.split("where")[0].trim();
			}
			filepath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasgroupby = true;
		} else if (query.contains("where")) {
			baseQuery = query.split("where")[0];
			conditionQuery = query.split("where")[1];
			conditionQuery = conditionQuery.trim();
			filepath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			this.relationalExpressionProcessing(conditionQuery);
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			haswhere = true;
		} else {
			baseQuery = query.split("from")[0].trim();
			filepath = query.split("from")[1].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasnormal = true;
		}

		return this;
	}

	public List<RelationalConditions> relationalExpressionProcessing(String relationalquery) {
		String oper[] = { ">=", "<=", "!=", ">", "<", "=" };

		for (String operator : oper) {
			if (relationalquery.contains(operator)) {
				relationalcondition.setColumn(relationalquery.split(operator)[0].trim());
				relationalcondition.setValue(relationalquery.split(operator)[1].trim());
				relationalcondition.setOperator(operator);
				relationalList.add(relationalcondition);
				break;
			}
		}
		if (relationalList.size() > 1)
			this.logicalOperatorFinder(relationalquery);
		return relationalList;
	}

	private void logicalOperatorFinder(String relationalquery) {
		String relationaldata[] = relationalquery.split(" ");

		for (String data : relationaldata) {
			if (data.trim().equals("and") || data.trim().equals("or")) {
				logicalOperator.add(data);
			}
		}
	}

}
