package com.stackroute.datamunging.testcase;

import static org.junit.Assert.assertNotNull;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;
import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.parsing.Query;

public class TestCase {

	static Query query;

	@BeforeClass
	public static void startPoint() {
		query = new Query();
	}

	@Test
	public void simpleQuery() throws Exception {
		DataCarrier dataSet = query.processorSelection("select * from E:/Emp.csv");
		assertNotNull(dataSet);
		display("select * from E:/Emp.csv", dataSet);
	}

	@Test
	public void simpleQueryWithSpecifiedColumnFields() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,Name,Salary from E:/Emp.csv");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv", dataSet);

	}

	@Test
	public void specifiedColumnQueryWithGreaterThanClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,Name,Salary from E:/Emp.csv where salary>30000");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv where salary>30000", dataSet);

	}

	@Test
	public void specifiedColumnQueryWithLessThanClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary<35000");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv where Salary<35000", dataSet);

	}

	@Test
	public void specifiedColumnQueryWithLessThanOrEqualClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary<=35000");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv where Salary<=35000", dataSet);

	}

	@Test
	public void specifiedColumnQueryWithGreaterThanOrEqualClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary>=35000");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv where Salary>=35000", dataSet);

	}

	@Test
	public void specifiedColumnQueryWithNotEqualClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary!=35000");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv where Salary!=35000", dataSet);

	}

	@Test
	public void specifiedColumnQueryWithEqualAndNotEqualClause() throws Exception {

		DataCarrier dataSet = query
				.processorSelection("select City,Name,Salary from E:/Emp.csv where City=Bangalore and Salary!=38000");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv where City=Bangalore and Salary!=38000", dataSet);

	}

	@Test
	public void aggregateFunctionCountClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select count(Name) from E:/Emp.csv");
		assertNotNull(dataSet);
		display("select count(Name) from E:/Emp.csv", dataSet);

	}

	@Test
	public void aggregateFunctionSumClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select sum(Salary) from E:/Emp.csv");
		assertNotNull(dataSet);
		display("select sum(Salary) from E:/Emp.csv", dataSet);

	}

	@Test
	public void aggregateFunctionSumClauseWithWhereEqualClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select sum(Salary) from E:/Emp.csv where City=Bangalore");
		assertNotNull(dataSet);
		display("select sum(Salary) from E:/Emp.csv where City=Bangalore", dataSet);

	}

	@Test
	public void specifiedColumnsWithOrderByClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,Name,Salary from E:/Emp.csv order by Salary");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv order by Salary", dataSet);

	}

	@Test
	public void specifiedColumnAndAggregateCountAllWithGroupByClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,count(*) from E:/Emp.csv group by City");
		assertNotNull(dataSet);
		displayGroupByRecords("select City,count(*) from E:/Emp.csv group by City", dataSet);

	}
	
	@Test
	public void selectColumnsWithWhereWithOrderByTestCase() throws Exception {

		DataCarrier dataSet = query
				.processorSelection("select City,Name,Salary from E:/Emp.csv where City=Bangalore order by Salary");
		assertNotNull(dataSet);
		display("select City,Name,Salary from E:/Emp.csv where City=Bangalore order by Salary", dataSet);

	}

	@Test
	public void specifiedColumnAndAggregateSumWithGroupByClause() throws Exception {

		DataCarrier dataSet = query.processorSelection("select City,sum(Salary) from E:/Emp.csv group by City");
		assertNotNull(dataSet);
		displayGroupByRecords("select City,sum(Salary) from E:/Emp.csv group by City", dataSet);
	}

	public void display(String testCaseName, DataCarrier printerDataSet) {
		System.out.println("===========================================\n");
		System.out.println(testCaseName);
		if (printerDataSet.getAggregateRow().isEmpty()) {
			for (RowDataHolder rowData : printerDataSet.getResultSet()) {
				Set<Integer> rowIndex = rowData.keySet();
				for (int index : rowIndex) {
					System.out.print(rowData.get(index) + "\t");
				}
				System.out.println();
			}
		} else {
			System.out.println();
			Set<String> columnNames = printerDataSet.getAggregateRow().keySet();

			for (String columnName : columnNames) {
				System.out.print(columnName + "\t");
			}
			System.out.println();
			for (String columnName : columnNames) {
				System.out.print(printerDataSet.getAggregateRow().get(columnName) + "\t");
			}
		}
	}

	public void displayGroupByRecords(String testCaseName, DataCarrier printerDataSet) {
		System.out.println("===========================================\n");
		System.out.println(testCaseName);
		LinkedHashMap<String, LinkedHashMap<String, Float>> groupRows = printerDataSet.getTotalGroupedData();
		Set<String> groupByColumnValues = groupRows.keySet();
		for (String groupByColumnValue : groupByColumnValues) {
			HashMap<String, Float> eachGroupRow = printerDataSet.getTotalGroupedData().get(groupByColumnValue);
			System.out.print(groupByColumnValue + "\t");
			Set<String> aggregateColumnNames = eachGroupRow.keySet();
			for (String eachAggregateColumnName : aggregateColumnNames) {
				System.out.print(eachGroupRow.get(eachAggregateColumnName)  + "\t");
			}
			System.out.println();
		}
	}

}
