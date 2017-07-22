package com.niit.test;

import static org.junit.Assert.*;

import org.junit.Before;

import com.niit.queryprogression.FileHandler;

import org.junit.*;
import java.util.*;
public class QueryManagerTest {
	FileHandler csv;

	@Before
	public void objectCreate() {
		csv = new FileHandler();
	}

	@Test
	public void selectAllWithoutWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select * from Emp.csv");
		assertNotNull(dataSet);
		display("selectAllWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,dept,name from Emp.csv");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,name,salary from Emp.csv where salary>30000");
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,name,salary from Emp.csv where salary<35000");
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase", dataSet);

	}

	@Test
	public void withWhereLessThanOrEqualToTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,name,salary from Emp.csv where salary<=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereGreaterThanOrEqualToTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,name,salary from Emp.csv where salary>=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereNotEqualToTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,name,salary from Emp.csv where salary!=35000");
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase", dataSet);

	}

	@Test
	public void withWhereEqualAndNotEqualTestCase() {

		Map<Integer, String> dataSet = csv
				.executeQuery("select city,name,salary from Emp.csv where city=Bangalore and salary<=40000");
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithOrderByTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,name,salary from Emp.csv orderby name");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithWhereWithOrderByTestCase() {

		Map<Integer, String> dataSet = csv
				.executeQuery("select city,name,salary from Emp.csv where city=Bangalore orderby name");
		assertNotNull(dataSet);
		display("selectColumnsWithWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectCountColumnsWithoutWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select count(name) from Emp.csv");
		assertNotNull(dataSet);
		display("selectCountColumnsWithoutWhereTestCase", dataSet);

	}

	@Test
	public void selectSumColumnsWithoutWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select sum(salary) from Emp.csv");
		assertNotNull(dataSet);
		display("selectSumColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectAvgColumnsWithoutWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select avg(salary) from Emp.csv");
		assertNotNull(dataSet);
		display("selectAvgColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectMinColumnsWithoutWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select min(salary) from Emp.csv");
		assertNotNull(dataSet);
		display("selectMinColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectMaxColumnsWithoutWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select max(salary) from Emp.csv");
		assertNotNull(dataSet);
		display("selectMaxColumnsWithoutWhereTestCase", dataSet);
	}

	@Test
	public void selectSumColumnsWithWhereTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select sum(salary) from Emp.csv where city=Bangalore");
		assertNotNull(dataSet);
		display("selectSumColumnsWithWhereTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupByCountTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select city,count(name) from Emp.csv groupby city");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereWithGroupBySumTestCase() {

		Map<Integer, String> dataSet = csv.executeQuery("select sum(salary) from Emp.csv where dept=IT ");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase", dataSet);

	}

	private void display(String testCaseName, Map<Integer, String> dataSet) {
		System.out.println("\n"+testCaseName);
		System.out.println("================================================================");
		for (String d : dataSet.values())
			System.out.println(d);
	}


}
