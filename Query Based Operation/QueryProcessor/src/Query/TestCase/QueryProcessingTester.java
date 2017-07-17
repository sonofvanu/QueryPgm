package Query.TestCase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import Query.QueryProctor.FileHandler;
import Query.QueryProctor.QueryParser;

public class QueryProcessingTester {

	String query2 = "select * from employee.csv where empid<12", query3 = "select * from employee.csv where empid&12";

	static FileHandler filehandler;
	static QueryParser queryparser;


	@Test(expected = NullPointerException.class)
	public void queryInsertionTest() {
		System.out.println("Test case 1 a started");

		for (String string : queryparser.inputQuerryArray(query2)) {
			System.out.println(string);
		}
		assertNotNull(queryparser.inputQuerryArray(query2).length);
		

	}

	@Test
	public void queryEligibilityChecker() {
		System.out.println("Test case 2 a started");
		queryparser.eligibleQuery("select * from employee.csv");
		assertTrue("im true", queryparser.eligibleQuery("select * from employee.csv"));
		System.out.println(queryparser.eligibleQuery("select * from employee.csv"));

		System.out.println("Test case 2 a completed");

	}

	//
	@Test(expected = Exception.class)
	public void queryEligibilityCheckerNegative() {
		System.out.println("Test case 2 b started");
		queryparser.eligibleQuery("select from employee.csv");
		assertFalse("im false", queryparser.eligibleQuery("select from employee.csv"));
		System.out.println(queryparser.eligibleQuery("select from employee.csv"));
		System.out.println("Test case 2 b completed");

	}
	// select * from employee.csv

	@Test(expected = Exception.class)
	public void relationalFieldProcessing() {
		System.out.println("Test case 4 a started");

		System.out.println(queryparser.relationalExpressionProcessing("empid<12"));
		assertNull("im not null", queryparser.relationalExpressionProcessing("empid<12"));
		System.out.println("Test case 4 a Completed");
	}

@Test(expected = Exception.class)
	public void relationalFieldProcessingNegative() {
	System.out.println("Test case 4 b started");
		System.out.println(queryparser.relationalExpressionProcessing("empid&12"));
		assertNotNull("im null", queryparser.relationalExpressionProcessing("empid&12"));
		System.out.println("Test case 4 b Completed");

	}

	@Test(expected = Exception.class)
	public void fieldsSeparator() {
		System.out.println("Test case 5 a started");

		System.out.println("Column Name: " + queryparser.fieldsSeparator(query2).column);
		System.out.println("Group by Col Name: " + queryparser.fieldsSeparator(query2).groupbycol
				+ "Order by Col Name: " + queryparser.fieldsSeparator(query2).orderbycol);
		assertNull("im not null", queryparser.fieldsSeparator(query2));
		System.out.println("Test case 5 a completed");

	}

	@Test(expected = Exception.class)
	public void fieldsSeparatorNegative() {
		System.out.println("Test case 5 b started");
		assertNull("im null", queryparser.fieldsSeparator(query3));

		System.out.println("Test case 5 b completed");

	}

	//
	@Test(expected = Exception.class)
	public void headerMap() {
		System.out.println("Test case 6 a started");

		System.out.println(filehandler.checkingHeaderInColumn(query2).toString());
		assertNotNull("im not null", filehandler.checkingHeaderInColumn(query2));
		System.out.println("Test case 6 a completed");
	}

	//
	@Test(expected = Exception.class)
	public void headerMapNegative() {
		System.out.println("Test case 6 b started");

		System.out.println(filehandler.checkingHeaderInColumn(query3).toString());
		assertNull("im null", filehandler.checkingHeaderInColumn(query3));
		System.out.println("Test case 6 b completed");
	}

	//
	@Test(expected = Exception.class)
	public void rowDataMap() {
		System.out.println("Test case 7 a started");

		System.out.println(filehandler.gettingRowDataMap().toString());
		assertNotNull("im not null", filehandler.gettingRowDataMap());
		System.out.println("Test case 7 a completed");
	}

	@Test(expected = Exception.class)
	public void rowDataMapNegative() {
		System.out.println("Test case 7 b started");

		System.out.println(filehandler.gettingRowDataMap().toString());
		assertNull("im null", filehandler.gettingRowDataMap());
		System.out.println("Test case 7 b completed");
	}

	//
	@Test(expected = Exception.class)
	public void columnDataProcessor() {
		System.out.println("Test case 8 a started");

		System.out.println(filehandler.ColumnDataProcessor(filehandler.columnSeparator(),
				filehandler.gettingRowDataMap(), filehandler.gettingHeaderMap()).toString());
		assertNotNull("im not null", filehandler.ColumnDataProcessor(filehandler.columnSeparator(),
				filehandler.gettingRowDataMap(), filehandler.gettingHeaderMap()));

		System.out.println("Test case 8 a completed");
	}

	@Test(expected = Exception.class)
	public void columnDataProcessorNegative() {
		System.out.println("Test case 8 b started");
		System.out.println(filehandler.ColumnDataProcessor(filehandler.columnSeparator(),
				filehandler.gettingRowDataMap(), filehandler.gettingHeaderMap()));
		assertNull("im null", filehandler.ColumnDataProcessor(filehandler.columnSeparator(),
				filehandler.gettingRowDataMap(), filehandler.gettingHeaderMap()));
		System.out.println("Test case 8 b completed");
	}

	@Test
	public void fieldsProcessor() {
		System.out.println("Test case 3 a started");
		System.out.println(queryparser.fieldsProcessing("empid,empname").columnames.colnames.toString());
		assertNotNull(queryparser.fieldsProcessing("empid,empname").columnames.colnames.get("empid"));
		System.out.println("Test case 3 a completed");
	}

	@Test(expected = NullPointerException.class)
	public void fetchingrowdata() throws Exception {
		System.out.println("Test 9 a started");
		System.out.println("The final data");
		for (String string : filehandler.fetchingRowData(filehandler.filename, query2, filehandler.relationalquery,filehandler.relationalList)) {
			System.out.println(string);
		}
		assertNotNull(filehandler.fetchingRowData(filehandler.filename, query2,filehandler.relationalquery,filehandler.relationalList));
		System.out.println("Test 9 a completed");
	}
	
	@Test
	public void whereQueryProcessing()
	{
		System.out.println("Test 10 a started");
		try {
			System.out.println(filehandler.whereQueryProcessing(queryparser, filehandler.headermap));
			assertNull("im null", filehandler.whereQueryProcessing(queryparser, filehandler.headermap));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	@BeforeClass
	public static void objectCreate() {
		filehandler = new FileHandler();
		queryparser = new QueryParser();
		
	}
	@Test
	public void selectAllWithoutWhereTestCase(){
		queryparser.inputQuerryArray("select * from employee.csv");
		Map<Integer,String> dataSet=filehandler.gettingRowDataMap();
		assertNotNull(dataSet);
		display("selectAllWithoutWhereTestCase",dataSet);
		
	}
	@Test
	public void selectColumnsWithoutWhereTestCase(){
		queryparser.inputQuerryArray("select empid,empname from employee.csv");
		Map<Integer,String> dataSet=filehandler.ColumnDataProcessor(filehandler.columns, filehandler.rowdatamap, filehandler.headermap);
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereTestCase",dataSet);
		
	}


	@Test(expected=Exception.class)
	public void withWhereGreaterThanTestCase(){
		queryparser.inputQuerryArray("select empid,empname from employee.csv where empsalary>30000");
		Map<Integer, String> dataSet;
		try {
			dataSet = filehandler.whereQueryProcessing(queryparser, filehandler.headermap);
			assertNotNull(dataSet);
			display("withWhereGreaterThanTestCase",dataSet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	@Test
	public void withWhereLessThanTestCase(){
		
		queryparser.inputQuerryArray("select empid,empname from employee.csv where empsalary<35000");
		Map<Integer, String> dataSet;
		try {
			dataSet = filehandler.whereQueryProcessing(queryparser, filehandler.headermap);
			assertNotNull(dataSet);
			display("withWhereGreaterThanTestCase",dataSet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	@Test
	public void withWhereLessThanOrEqualToTestCase(){
		queryparser.inputQuerryArray("select empid,empname from employee.csv where empsalary<=35000");
		Map<Integer, String> dataSet;
		try {
			dataSet = filehandler.whereQueryProcessing(queryparser, filehandler.headermap);
			assertNotNull(dataSet);
			display("withWherelessThanTestCaseorequal",dataSet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	@Test
	public void withWhereGreaterThanOrEqualToTestCase(){
		
		
		queryparser.inputQuerryArray("select empid,empdept,empname from employee.csv where empsalary>=35000");
		Map<Integer, String> dataSet;
		try {
			dataSet = filehandler.whereQueryProcessing(queryparser, filehandler.headermap);
			assertNotNull(dataSet);
			display("withWhereGreaterThanTestCase",dataSet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	
	@Test
	public void withWhereNotEqualToTestCase(){
		
		queryparser.inputQuerryArray("select empid,empname from employee.csv where empsalary!=35000");
		Map<Integer, String> dataSet;
		try {
			dataSet = filehandler.whereQueryProcessing(queryparser, filehandler.headermap);
			assertNotNull(dataSet);
			display("withWhereGreaterThanTestCase",dataSet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Test
	public void withWhereEqualAndNotEqualTestCase(){
		queryparser.inputQuerryArray("select city,name,salary from d:/emp.csv where salary>=30000 and salary<=38000");
		Map<Integer, String> dataSet;
		try {
			dataSet = filehandler.whereQueryProcessing(queryparser, filehandler.headermap);
			assertNotNull(dataSet);
			display("withWhereGreaterThanTestCase",dataSet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void display(String testCaseName,Map<Integer, String> dataSet){
		System.out.println(testCaseName);
		System.out.println("================================================================");
		System.out.println(dataSet);
	}
}
