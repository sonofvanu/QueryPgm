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
	public void checkingTest() {
		Map<Integer, String> resultantSet = processController.queryFunctionsCaller("select * from employee.csv");
		assertNotNull(resultantSet);
		show("multipleTest", resultantSet);
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