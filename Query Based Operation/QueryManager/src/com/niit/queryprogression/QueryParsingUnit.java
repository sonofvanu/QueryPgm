package com.niit.queryprogression;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParsingUnit {

	private List<String> cols = new ArrayList<>();
	private String path;
	private boolean hasAggregate;
	private List<RestrictionConditions> condition = new ArrayList<>();
	private List<String> operators = new ArrayList<>();
	private String grp_clause;
	private String ord_clause;
	private int type1, type2, type3, type4;

	

	public boolean isHasAggregate() {
		return hasAggregate;
	}

	public void setHasAggregate(boolean hasAggregate) {
		this.hasAggregate = hasAggregate;
	}

	public List<String> getOperators() {
		return operators;
	}

	public void setOperators(List<String> operators) {
		this.operators = operators;
	}

	public int getType1() {
		return type1;
	}

	public void setType1(int type1) {
		this.type1 = type1;
	}

	public int getType2() {
		return type2;
	}

	public void setType2(int type2) {
		this.type2 = type2;
	}

	public int getType3() {
		return type3;
	}

	public void setType3(int type3) {
		this.type3 = type3;
	}

	public int getType4() {
		return type4;
	}

	public void setType4(int type4) {
		this.type4 = type4;
	}

	public List<String> getCols() {
		return cols;
	}

	public void setCols(List<String> cols) {
		this.cols = cols;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public List<RestrictionConditions> getCondition() {
		return condition;
	}

	public void setCondition(List<RestrictionConditions> condition) {
		this.condition = condition;
	}

	public String getGrp_clause() {
		return grp_clause;
	}

	public void setGrp_clause(String grp_clause) {
		this.grp_clause = grp_clause;
	}

	public String getOrd_clause() {
		return ord_clause;
	}

	public void setOrd_clause(String ord_clause) {
		this.ord_clause = ord_clause;
	}

	private void checkaggregate() {

		for (String col : cols) {
			if (col.contains("sum") || col.contains("count") || col.contains("avg") || col.contains("min")
					|| col.contains("max")) {
				hasAggregate = true;
			} else
				hasAggregate = false;

		}
	}

	public QueryParsingUnit validateThruRegex(String query) {
		String queryHolder = query;
		String splitQuery[];

		Pattern pat1 = Pattern.compile("select\\s.*from\\s.*");
		Matcher mat1 = pat1.matcher(queryHolder);
		if (mat1.find()) {
			if (queryHolder.contains("groupby")) {
				type4 = 4;
				splitQuery = queryHolder.split("groupby");
				grp_clause = splitQuery[1].replaceAll("\\s", "");
				queryHolder = splitQuery[0];

			}
			if (queryHolder.contains("orderby")) {
				type3 = 3;
				splitQuery = queryHolder.split("orderby");
				ord_clause = splitQuery[1].replaceAll("\\s", "");
				queryHolder = splitQuery[0];

			}
			if (queryHolder.contains("where")) {
				List<String> operands = new ArrayList<>();
				type2 = 2;
				String wholeWhere = "";
				splitQuery = queryHolder.split("where");
				wholeWhere = splitQuery[1];

				String holder = wholeWhere;
				StringBuilder sb = new StringBuilder();
				for (String s : holder.split(" ")) {

					if (!s.equals("")) // ignore space
						sb.append(s + " "); // add word with 1 space

				}
				holder = sb.toString();
				sb.setLength(0);
				String splitHolder[] = holder.split(" ");
				int si = splitHolder.length;
				String splitNew[] = new String[si + 1];
				for (int s = 0; s < si; s++) {
					splitNew[s] = splitHolder[s];
				}
				splitNew[si] = "dummy";

				for (String st : splitNew) {
					if (!st.equals("and") && !st.equals("or") && !st.equals("dummy")) {
						sb.append(st);
					} else if (st.equals("and")) {
						operators.add("and");
						operands.add(sb.toString());
						sb.setLength(0);
					} else if (st.equals("or")) {
						operators.add("or");
						operands.add(sb.toString());
						sb.setLength(0);
					} else if (st.equals("dummy")) {
						operands.add(sb.toString());
						sb.setLength(0);
					}

				}

				int len = operands.size();
				RestrictionConditions conditionObj[] = new RestrictionConditions[len];
				for (int assign = 0; assign < len; assign++) {
					conditionObj[assign] = new RestrictionConditions();
					String conditionsplit[] = operands.get(assign).split("[\\s]*[>=|<=|!=|=|<|>][\\s]*");
					int l = conditionsplit.length;

					if (l == 3) {
						conditionObj[assign].setCol_name(conditionsplit[0]);
						conditionObj[assign].setValue(conditionsplit[2]);
						if (operands.get(assign).contains(">="))
							conditionObj[assign].setOperator(">=");
						if (operands.get(assign).contains("<="))
							conditionObj[assign].setOperator("<=");
						if (operands.get(assign).contains("!="))
							conditionObj[assign].setOperator("!=");
					} else if (l == 2) {
						conditionObj[assign].setCol_name(conditionsplit[0]);
						conditionObj[assign].setValue(conditionsplit[1]);
						if (operands.get(assign).contains(">"))
							conditionObj[assign].setOperator(">");
						if (operands.get(assign).contains("<"))
							conditionObj[assign].setOperator("<");
						if (operands.get(assign).contains("="))
							conditionObj[assign].setOperator("=");
					}
					condition.add(conditionObj[assign]);

				}
				queryHolder = splitQuery[0];
			}
			if (queryHolder.contains("from")) {
				type1 = 1;
				splitQuery = queryHolder.split("from");
				path = "G:\\" + splitQuery[1].replaceAll("\\s", "");
				queryHolder = splitQuery[0];
			}
			if (queryHolder.contains("select")) {
				splitQuery = queryHolder.split("select");
				String columns = splitQuery[1].replaceAll("\\s", "");
				if (!columns.equals("*")) {
					String splitedColname[] = columns.trim().split(",");
					for (int c = 0; c < splitedColname.length; c++) {
						cols.add(splitedColname[c]);
					}

					checkaggregate();
				} else {
					cols.add("*");
				}
				queryHolder = splitQuery[0];
			}

		}

		return this;
	}

}
