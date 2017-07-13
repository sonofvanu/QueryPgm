package Query.QueryProctor;

public class QueryParser {

	public String filepath, queryfields, querycondition;
	public String orderbycol, groupbycol, selectcol;
	QueryInput queryinput = new QueryInput();
	public String query = queryinput.queryGetter();
	private RelationalConditions relationalcondition = new RelationalConditions();
	private boolean hasgroupby, hasorderby, haswhere, hasAllColumn;
	private ColumnName columnames = new ColumnName();
	public String basequery = null, conditionqry = null;
	
	public String[] inputQuerryArray() {

		String[] querryarray = query.split(" ");
		return querryarray;
	}

	public boolean eligibleQuery(String query) {
		if (query.contains("select") && query.contains("from") && query.contains("*") || query.contains("where")
				|| query.contains("group by") || query.contains("order by") || query.contains("sort by")) {
			//System.out.println("Entered a valid query...");
			

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
				System.out.println(basequery);
				if (basequery.contains("where")) {
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
			} else if (query.contains("where")) {
				basequery = query.split("where")[0];
				conditionqry = query.split("where")[1];
				conditionqry = conditionqry.trim();
				filepath = basequery.split("from")[1].trim();
				String relationalqry = conditionqry.split("and|or")[0].trim();
				System.out.println(relationalqry);
				this.relationalExpressionProcessing(relationalqry);
				selectcol = basequery.split("select")[1].trim();
				this.fieldsProcessing(selectcol);
				haswhere = true;
			} else {
				basequery = query.split("from")[0].trim();
				//filepath = basequery.split("from")[1].trim();
				selectcol = basequery.split("select")[1].trim();
				this.fieldsProcessing(selectcol);
			}

			

			return true;
		} else {
			System.out.println("Entered an in-valid query...");
			return false;
		}

	}

	private boolean fieldsProcessing(String selectcolumn) {
		if (selectcolumn.trim().contains("*") && selectcolumn.length() == 1) {
			hasAllColumn = true;
			return hasAllColumn;

		} else {
			String columnlist[] = selectcolumn.split(",");

			int i = 0;
			for (String column : columnlist) {
				columnames.colnames.put(column, i);
				i++;
			}
			return hasAllColumn = false;
		}
	}

	private RelationalConditions relationalExpressionProcessing(String relationalquery) {
		String oper[] = { ">", "<", ">=", "<=", "=", "!=" };

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
