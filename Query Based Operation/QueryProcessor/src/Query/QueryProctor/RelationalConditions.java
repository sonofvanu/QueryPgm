package Query.QueryProctor;

import java.util.LinkedList;
import java.util.List;

public class RelationalConditions {

	private String column, value, operator;

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}
	
	List<RelationalConditions> list=new LinkedList<>();
}
