package Query.TestCase;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import Query.QueryProctor.FileHandler;
import Query.QueryProctor.QueryParser;
import Query.QueryProctor.RelationalConditions;

public class QueryProcessingTester {

	String query2 = "select * from employee.csv where empid<12";

	QueryParser queryparser = new QueryParser();
	FileHandler filehandler=new FileHandler();
	// private RelationalConditions relationalcondition = new
	// RelationalConditions();

	@Test
	public void queryInsertionTest() {

		System.out.println("im in");
		// String[] expectedarray={};
		assertNotNull(queryparser.inputQuerryArray());
		for (String printer : queryparser.inputQuerryArray()) {
			System.out.println(printer);
		}
		// assertArrayEquals(expectedarray, queryparser.inputQuerryArray());

	}

	// @Test
	// public void queryInsertionTestNegative() {
	//
	// System.out.println("im in");
	// assertEquals("This is a failure case", >0,
	// queryparser.inputQuerryArray().length);
	// for (String string : queryparser.inputQuerryArray()) {
	// System.out.println(string);
	// }
	// System.out.println("Test case 1 completed");
	// }

	@SuppressWarnings("unused")
	private void assertFalse(QueryParser queryParser2) {

	}

	@Test
	public void queryEligibilityChecker() {
		System.out.println("Im in on");
		queryparser.eligibleQuery("select * from employee.csv");
		assertTrue(queryparser.eligibleQuery("select * from employee.csv"));
		System.out.println(queryparser.eligibleQuery("select * from employee.csv"));
		System.out.println("Test case 2 completed");

	}

	@BeforeClass
	public static void welcome() {
		System.out.println("starting testing");
	}

	// @Before
	// public void nextCase() {
	// System.out.println("Output of the Case:");
	// }

	@Test
	public void fieldsProcessing() {
		assertNotNull(queryparser.fieldsProcessing("empid,empname"));
		System.out.println(queryparser.fieldsProcessing("empid,empname").columnames.colnames);
		System.out.println("Test case 3 completed");
	}

	@Test
	public void relationalFieldProcessing() {

		assertNotNull(queryparser.relationalExpressionProcessing("empid<12"));
		System.out.println("Column Name: " + queryparser.relationalExpressionProcessing("empid<12").getColumn());
		System.out.println(
				"Relational Operator: " + queryparser.relationalExpressionProcessing("empid<12").getOperator());
		System.out.println("Value: " + queryparser.relationalExpressionProcessing("empid<12").getValue());
		System.out.println("Test case 4 Completed");

	}

	@Test
	public void fieldsSeparator() {
		assertFalse(queryparser.fieldsSeparator(query2));
		System.out.println("Column Name: " + queryparser.fieldsSeparator(query2).column);
		System.out.println("Group by Col Name: " + queryparser.fieldsSeparator(query2).groupbycol);
		System.out.println("Order by Col Name: " + queryparser.fieldsSeparator(query2).orderbycol);
		System.out.println("Selected Col Name: " + queryparser.fieldsSeparator(query2).selectcol);
		System.out.println("Filepath: " + queryparser.fieldsSeparator(query2).filepath);
		System.out.println("Query Condition: " + queryparser.fieldsSeparator(query2).querycondition);
		System.out.println("Query Fields: " + queryparser.fieldsSeparator(query2).queryfields);
		System.out.println("Test case 5 completed");

	}
	
	@Test
	public void headerMap()
	{
		assertNotNull(filehandler.checkingHeaderInColumn(query2));
		System.out.println(filehandler.checkingHeaderInColumn(query2));
	}
	
	@Test
	public void rowDataMap()
	{
		assertNotNull(filehandler.gettingRowDataMap());
		System.out.println(filehandler.gettingRowDataMap());
	}
	
	public void columnDataProcessor()
	{
	assertNotnull(filehandler.ColumnDataProcessor(filehandler.columnSeparator(), filehandler.gettingRowDataMap(), filehandler.gettingHeaderMap()));
	System.out.println(filehandler.ColumnDataProcessor(filehandler.columnSeparator(), filehandler.gettingRowDataMap(), filehandler.gettingHeaderMap()));
	}

	private void assertNotnull(Map<Integer, String> columnDataProcessor) {
		// TODO Auto-generated method stub
		
	}

}
