package Query.QueryProctor;

public class QueryParser {

	public static String filepath, queryfields, querycondition, querryarray[];
	public String orderbycol, groupbycol, column;
	QueryInput queryinput = new QueryInput();
	public String query = queryinput.queryGetter();
	private RelationalConditions relationalcondition = new RelationalConditions();
	private boolean hasgroupby, hasorderby, haswhere, hasAllColumn;
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
				i++;
			}

			// System.out.println(columnames.colnames);

		}
		return this;
	}

	public QueryParser fieldsSeparator(String query) {
		String basequery = null, conditionqry = null;

		if (query.contains("order by")) {
			basequery = query.split("order by")[0].trim();
			orderbycol = query.split("order by")[1].trim();
			filepath = basequery.split("from")[1].trim();
			basequery = basequery.split("from")[0].trim();
			selectcol = basequery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasorderby = true;
		}

		if (query.contains("group by")) {
			basequery = query.split("group by")[0].trim();
			groupbycol = query.split("group by")[1].trim();
			if (query.contains("where")) {
				conditionqry = basequery.split("where")[1].trim();
				String relationalqry = conditionqry.split("and|or")[0].trim();
				this.relationalExpressionProcessing(relationalqry);
				basequery = basequery.split("where")[0].trim();
			}
			filepath = basequery.split("from")[1].trim();
			basequery = basequery.split("from")[0].trim();
			selectcol = basequery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			hasgroupby = true;
		}

		if (query.contains("where")) {
			basequery = query.split("where")[0];
			conditionqry = query.split("where")[1];
			conditionqry = conditionqry.trim();
			filepath = basequery.split("from")[1].trim();
			String relationalqry = conditionqry.split("and|or")[0].trim();
			this.relationalExpressionProcessing(relationalqry);
			selectcol = basequery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
			haswhere = true;
		}

		else {
			basequery = query.split("from")[0].trim();
			filepath = query.split("from")[1].trim();
			selectcol = basequery.split("select")[1].trim();
			this.fieldsProcessing(selectcol);
		}

		FileHandler filehandler = new FileHandler();
		try {
			filehandler.fetchingRowData(filepath);
			// filehandler.checkingHeaderInColumn(query);
		} catch (Exception e) {

			e.printStackTrace();
		}

		return this;
	}

	public RelationalConditions relationalExpressionProcessing(String relationalquery) {
		String oper[] = { ">=", "<=", "!=", ">", "<", "=" };

		for (String operator : oper) {
			if (relationalquery.contains(operator)) {
				relationalcondition.setColumn(relationalquery.split(operator)[0].trim());
				relationalcondition.setValue(relationalquery.split(operator)[1].trim());
				relationalcondition.setOperator(operator);

				break;
			}
		}
		return relationalcondition;
	}

}
