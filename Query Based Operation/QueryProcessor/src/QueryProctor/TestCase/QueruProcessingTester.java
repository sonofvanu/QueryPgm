package QueryProctor.TestCase;

import static org.junit.Assert.*;

import org.junit.Test;

import Query.QueryProctor.QueryParser;

public class QueruProcessingTester {

	String query1="select * from employee.csv";
	String query2="select * from employee.csv where empid<12";
	String query3="select * from employee.csv where empsalary<18000 order by empdept";
	QueryParser queryparser = new QueryParser();
	
	@Test
	public void querinsertiontest() {
		
		queryparser.inputQuerryArray();
		System.out.println("im in");
		fail("oh no");
	}
	
	@Test
	public void queryeligibility()
	{
		queryparser.eligibleQuery(query1);
		System.out.println("query eliibility checked");
		fail("oh no1");
	}

}
