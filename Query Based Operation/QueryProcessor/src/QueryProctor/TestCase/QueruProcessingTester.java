package QueryProctor.TestCase;

import static org.junit.Assert.*;

import org.junit.Test;

import Query.QueryProctor.QueryParser;
import Query.QueryProctor.RelationalConditions;

public class QueruProcessingTester {

	String query1="select * from employee.csv";
	String query2="select * from employee.csv where empid<12";
	String query3="select * from employee.csv where empsalary<18000 order by empdept";
	QueryParser queryparser = new QueryParser();
	private RelationalConditions relationalcondition = new RelationalConditions();
	
	
	@Test
	public void querinsertiontest() {
		
		System.out.println("im in");
		for(String printer:queryparser.inputQuerryArray())
		{
			System.out.println(printer);
		}
	}
	
	@Test
	public void queryeligibilitychecker()
	{
		queryparser.eligibleQuery(query2);
		System.out.println(relationalcondition.getColumn());
		System.out.println(relationalcondition.getOperator());
		System.out.println(relationalcondition.getValue());
		
	}
	

}
