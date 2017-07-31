package com.stackroute.datamunging.testcase;

import static org.junit.Assert.assertNotNull;

import java.util.LinkedHashMap;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.model.RowDataHolder;
import com.stackroute.datamunging.parsing.QueryEligibilityChecker;

public class TestCase {

	static QueryEligibilityChecker queryEligibilityChecker;

	@BeforeClass
	public static void objectMaintainer() {
		queryEligibilityChecker = new QueryEligibilityChecker();
	}

	@Test
	public void selectAllWithoutWhereTestCase() throws Exception {
		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select * from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectAllWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select City,Name,Salary from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker
				.processorSelection("select City,Name,Salary from E:/Emp.csv where salary>30000");
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker
				.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary<35000");
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanOrEqualToTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker
				.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary<=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanOrEqualToTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary>=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereNotEqualToTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary<=35000");
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereEqualAndNotEqualTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select City,Name,Salary from E:/Emp.csv where Salary>=30000 and Salary<=38000");
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase", dataSet);

	}

	@Test
	public void selectCountColumnsWithoutWhereTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select count(Name) from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectCountColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectSumColumnsWithoutWhereTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select sum(Salary) from E:/Emp.csv");
		assertNotNull(dataSet);
		display("selectSumColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectSumColumnsWithWhereTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select sum(Salary) from E:/Emp.csv where City=Bangalore");
		assertNotNull(dataSet);
		display("selectSumColumnsWithWhereTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithOrderByTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select City,Name,Salary from E:/Emp.csv order by Salary");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithWhereWithOrderByTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select City,Name,Salary from E:/Emp.csv where City=Bangalore order by Salary");
		assertNotNull(dataSet);
		display("selectColumnsWith-WhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupByCountTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker.processorSelection("select City,count(*) from E:/Emp.csv group by City");
		assertNotNull(dataSet);
		displayGroupByRecords("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupBySumTestCase() throws Exception {

		DataCarrier dataSet = queryEligibilityChecker
				.processorSelection("select City,sum(Salary) from E:/Emp.csv group by City");
		assertNotNull(dataSet);
		displayGroupByRecords("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	public void display(String str, DataCarrier dataSet) {
		System.out.println();
		System.out.println(str);
		System.out.println();

		if (dataSet.getAggregateRow().isEmpty()) {

			for (RowDataHolder rowData : dataSet.getResultSet()) {
				Set<Integer> rowIndex = rowData.keySet();

				for (int index : rowIndex) {
					System.out.print(rowData.get(index) + "\t");
				}

				System.out.println();
			}

		} else {
			System.out.println();
			Set<String> columnNames = dataSet.getAggregateRow().keySet();

			for (String columnName : columnNames) {
				System.out.print(columnName + "\t");
			}
			System.out.println();
			for (String columnName : columnNames) {
				System.out.print(dataSet.getAggregateRow().get(columnName) + "\t");
			}
		}
	}

	public void displayGroupByRecords(String str, DataCarrier dataSet4) {
		System.out.println();
		System.out.println(str);
		System.out.println();

		LinkedHashMap<String, LinkedHashMap<String, Float>> groupRows = dataSet4.getTotalGroupedData();

		Set<String> groupByColumnValues = groupRows.keySet();

		for (String groupByColumnValue : groupByColumnValues) {
			LinkedHashMap<String, Float> eachGroupRow = dataSet4.getTotalGroupedData().get(groupByColumnValue);
			System.out.print(groupByColumnValue + "\t");
			Set<String> aggregateColumnNames = eachGroupRow.keySet();
			for (String eachAggregateColumnName : aggregateColumnNames) {
				System.out.print(eachGroupRow.get(eachAggregateColumnName) + "\t");
			}

			System.out.println();
		}

	}

}
