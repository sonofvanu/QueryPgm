package com.niit.testcase;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.niit.queryparsingunit.ProcessorController;

public class QueryTestCase {
	ProcessorController processController;

	@Before
	public void ObjectCreator() {
		processController = new ProcessorController();
	}

	@Test
	public void simpleTest() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv");
		assertNotNull(resultantSet);
		show("simpleTest", resultantSet);
	}
	
	@Test
	public void columnTest() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select empid,empsalary from employee.csv");
		assertNotNull(resultantSet);
		show("columnTest", resultantSet);
	}

	@Test
	public void whereSingleGreater() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv where empid>3");
		assertNotNull(resultantSet);
		show("whereSingleGreater", resultantSet);
	}
	
	
	@Test
	public void whereSingleLesser() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv where empsalary<45000");
		assertNotNull(resultantSet);
		show("whereSingleLesser", resultantSet);
	}
	
	@Test
	public void whereSingleEqual() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv where empname=Anand");
		assertNotNull(resultantSet);
		show("whereSingleEqual", resultantSet);
	}
	
	@Test
	public void whereSingleNotEqual() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv where empsalary!=60000");
		assertNotNull(resultantSet);
		show("whereSingleNotEqual", resultantSet);
	}
	
	@Test
	public void whereSingleLessEqual() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv where empsalary>=60000");
		assertNotNull(resultantSet);
		show("whereSingleLessEqual", resultantSet);
	}
	
	@Test
	public void whereSingleGreatEqual() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv where empid<=5");
		assertNotNull(resultantSet);
		show("whereSingleGreatEqual", resultantSet);
	}
	
	@Test
	public void whereMultiple() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv where empid<3 and empsalary>50000 or empdept=delivery");
		assertNotNull(resultantSet);
		show("whereMultiple", resultantSet);
	}
	
	public void show(String msg, Map<Integer, String> resultSet) {
		System.out.println("================================================");
		System.out.println(msg);
		for (String string : resultSet.values()) {
			System.out.println(string);
		}
		System.out.println("================================================");
	}

}