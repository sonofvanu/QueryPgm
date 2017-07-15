package Query.QueryProctor;

import java.util.ArrayList;
import java.util.*;

public class QueryParser {

	public static String filepath, queryfields, querycondition, querryarray[];
	public String relationalquery;
	public String orderbycol, groupbycol, column;
	QueryInput queryinput = new QueryInput();
	public String query;// = queryinput.queryGetter();

	public List<RelationalConditions> relationalList = new ArrayList<>();
	private boolean hasgroupby, hasorderby, haswhere, hasAllColumn, hasnormal, hasAggregate;
	private List<String> logicalOperator = new ArrayList<String>();
	public List<String> columnnamelist = new ArrayList<>();

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

	public String[] inputQuerryArray(String query) {
		this.query = query;
		querryarray = query.split(" ");
		this.eligibleQuery(query);
		// column = querryarray[1];
		// System.out.println(column);
		return querryarray;
	}

	public boolean eligibleQuery(String query) {
		if ((query.contains("select") && query.contains("from")) || ((query.contains("*") || query.contains("where")
				|| query.contains("group by") || query.contains("order by") || query.contains("sort by")))) {
			System.out.println("this is a valid query");
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
		String baseQuery = null, relationalquery = null, selectcol = null;

		if (query.contains("order by")) {
			baseQuery = query.split("order by")[0].trim();
			orderbycol = query.split("order by")[1].trim();
			if (baseQuery.contains("where")) {
				relationalquery = baseQuery.split("where")[1].trim();
				this.relationalExpressionProcessing(relationalquery);
				baseQuery = baseQuery.split("where")[0].trim();
				haswhere = true;
			}
			filepath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasorderby = true;
		} else if (query.contains("group by")) {
			baseQuery = query.split("group by")[0].trim();
			groupbycol = query.split("group by")[1].trim();
			if (baseQuery.contains("where")) {
				relationalquery = baseQuery.split("where")[1].trim();
				this.relationalExpressionProcessing(relationalquery);
				baseQuery = baseQuery.split("where")[0].trim();
				haswhere = true;
			}
			filepath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasgroupby = true;
		} else if (query.contains("where")) {
			baseQuery = query.split("where")[0];
			relationalquery = query.split("where")[1];
			relationalquery = relationalquery.trim();
			filepath = baseQuery.split("from")[1].trim();
			baseQuery = baseQuery.split("from")[0].trim();
			this.relationalExpressionProcessing(relationalquery);
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			haswhere = true;
		} else {
			baseQuery = query.split("from")[0].trim();
			filepath = query.split("from")[1].trim();
			selectcol = baseQuery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasAllColumn = true;
		}
		System.out.println("im separating fields");
		// System.out.println("the base query is " + baseQuery);
		System.out.println("the file path is " + filepath);
		if (hasorderby)
			System.out.println("the order by col is " + orderbycol);
		System.out.println("the selct by col is " + selectcol);
		if (hasgroupby)
			System.out.println("the group by is " + groupbycol);
		System.out.println("the relational query is " + relationalquery);
		System.out.println("completed separating fileds");
		Iterator i = relationalList.iterator();
		
		while (i.hasNext()) {
			RelationalConditions rc = (RelationalConditions) i.next();
			System.out.println(rc.getColumn() + "  -------  " + rc.getOperator() + "  --------  " + rc.getValue());
		}

		FileHandler filehandler = new FileHandler();
		try {
			filehandler.fetchingRowData(filepath, query,relationalquery,relationalList);
		} catch (Exception e) {

			e.printStackTrace();
		}
		return this;
	}

	public List<RelationalConditions> relationalExpressionProcessing(String relationalquery) {
		String oper[] = { ">=", "<=", "!=", ">", "<", "=" };
		System.out.println("the obtained relational query is " + relationalquery);
		String[] relationalquerylist = relationalquery.split("and|or");

		for (String relquery : relationalquerylist) {
			relquery = relquery.trim();
			for (String operator : oper) {
				if (relquery.contains(operator)) {
					RelationalConditions relationalcondition = new RelationalConditions();
					relationalcondition.setColumn(relquery.split(operator)[0].trim());
					relationalcondition.setValue(relquery.split(operator)[1].trim());
					relationalcondition.setOperator(operator);
					relationalList.add(relationalcondition);
					break;
				}
			}
		}

		if (relationalList.size() > 1) {
			this.logicalOperatorFinder(relationalquery);
		}
		return relationalList;
	}

	private void logicalOperatorFinder(String relationalquery) {
		String relationaldata[] = relationalquery.split(" ");

		for (String data : relationaldata) {
			if (data.trim().equals("and") || data.trim().equals("or")) {
				logicalOperator.add(data);
			}
		}
		Iterator i = logicalOperator.iterator();
		while (i.hasNext()) {
			System.out.println(i.next());
		}
	}

}
