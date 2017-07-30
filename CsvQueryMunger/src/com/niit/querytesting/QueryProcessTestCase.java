package com.niit.querytesting;

import static org.junit.Assert.assertNotNull;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.niit.queryoperation.RowData;
import com.niit.queryparse.QueryChecking;
import com.niit.queryoperation.DataSetter;

public class QueryProcessTestCase 
{
	static QueryChecking queryChecker;

	@BeforeClass
	public static void initialize()
	{
		queryChecker=new QueryChecking();
	}
	

	public void allColumnsAllRows()throws Exception
	{
		String queryString="select * from D:/Emp.csv";
		assertNotNull(queryChecker.executeQuery(queryString));
		System.out.println(queryString);
		displayRecords(queryChecker.executeQuery(queryString));
	}
	
	@Test
	public void specificColumnsAllRows()throws Exception
	{
		String queryString="select Name,City,Salary from D:/Emp.csv";
		assertNotNull(queryChecker.executeQuery(queryString));
		System.out.println(queryString);
		displayRecords(queryChecker.executeQuery(queryString));
	
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void allColumnsSpecificRows()throws Exception
	{
		String queryString="select * from D:/Emp.csv where Name = Anand";
		DataSetter dataSet=queryChecker.executeQuery(queryString);
		assertNotNull(dataSet.getResultSet().size());
		System.out.println(queryString);
		displayRecords(dataSet);
		
		String queryString1="select * from  D:/Emp.csv where Salary>=35000";
		DataSetter dataSet1=queryChecker.executeQuery(queryString1);
		assertNotNull(dataSet1.getResultSet().get(0));
		System.out.println(queryString1);
		displayRecords(dataSet1);
		
		String queryString2="select * from D:/Emp.csv where Salary>31000 or City = Bangalore and Name=Vinod";
		DataSetter dataSet2=queryChecker.executeQuery(queryString2);
		assertNotNull(dataSet2.getResultSet().get(0));
		System.out.println(queryString2);
		displayRecords(dataSet2);
	}
	
	@Test
	public void orderByClause()throws Exception
	{
		String queryString2="select * from D:/Emp.csv order by Salary";
		DataSetter dataSet2=queryChecker.executeQuery(queryString2);
		assertNotNull(dataSet2.getResultSet().get(0));
		System.out.println(queryString2);
		displayRecords(dataSet2);
	}
	
	@Test
	public void selectAllWithoutWhereTestCase(){
		
		DataSetter dataSet=queryChecker.executeQuery("select * from d:/emp.csv");
		assertNotNull(dataSet);
		display("selectAllWithoutWhereTestCase",dataSet);
		
	}

	@Test
	public void selectColumnsWithoutWhereTestCase(){
		
		DataSetter dataSet=queryChecker.executeQuery("select Name,City,Salary from D:/Emp.csv");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereGreaterThanTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select City,Name,Salary from d:/emp.csv where Salary>30000");
		assertNotNull(dataSet);
		display("withWhereGreaterThanTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereLessThanTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select City,Name,Salary from d:/emp.csv where Salary<35000");
		assertNotNull(dataSet);
		display("withWhereLessThanTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereLessThanOrEqualToTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select City,Name,Salary from d:/emp.csv where Salary<=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}

	@Test
	public void withWhereGreaterThanOrEqualToTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select city,name,salary from d:/emp.csv where salary>=35000");
		assertNotNull(dataSet);
		display("withWhereLessThanOrEqualToTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereNotEqualToTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select city,name,salary from d:/emp.csv where salary>=35000");
		assertNotNull(dataSet);
		display("withWhereNotEqualToTestCase",dataSet);
		
	}
	
	@Test
	public void withWhereEqualAndNotEqualTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select City,Name,Salary from d:/emp.csv where Salary>=30000 and Salary<=38000");
		assertNotNull(dataSet);
		display("withWhereEqualAndNotEqualTestCase",dataSet);
		
	}
	
	@Test
	public void selectCountColumnsWithoutWhereTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select count(name) from d:/emp.csv");
		assertNotNull(dataSet);
		display("selectCountColumnsWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void selectSumColumnsWithoutWhereTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select sum(salary) from d:/emp.csv");
		assertNotNull(dataSet);
		display("selectSumColumnsWithoutWhereTestCase",dataSet);
		
	}
	
	@Test
	public void selectSumColumnsWithWhereTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select sum(salary) from d:/emp.csv where city=Bangalore");
		assertNotNull(dataSet);
		display("selectSumColumnsWithWhereTestCase",dataSet);
		
	}
	
	@Test
	public void selectColumnsWithoutWhereWithOrderByTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select city,name,salary from d:/emp.csv order by salary");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	


	@Test
	public void selectColumnsWithWhereWithOrderByTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select city,name,salary from d:/emp.csv where city=Bangalore order by salary");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	@Test
	public void selectColumnsWithoutWhereWithGroupByCountTestCase(){
		
		DataSetter  dataSet=queryChecker.executeQuery("select city,count(*) from d:/emp.csv group by city");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	@Test
	public void selectColumnsWithoutWhereWithGroupBySumTestCase(){
		
		DataSetter dataSet=queryChecker.executeQuery("select city,sum(salary) from d:/emp.csv group by city");
		assertNotNull(dataSet);
		display("selectColumnsWithoutWhereWithOrderByTestCase",dataSet);
		
	}
	
	
	public void displayRecords(DataSetter dataSet)
	{
		for(RowData rowData:dataSet.getResultSet())
		{
			Set<Integer> rowIndex=rowData.keySet();
			
			for(int index:rowIndex)
			{
				System.out.print(rowData.get(index)+"\t\t");
			}
			
			System.out.println();
		}
	}
	
	
	private void display(String string, DataSetter dataSet) {
		// TODO Auto-generated method stub
		System.out.println(string);
		System.out.println("================================================================");
		
		for(RowData rowData:dataSet.getResultSet())
		{
			Set<Integer> rowIndex=rowData.keySet();
			
			for(int index:rowIndex)
			{
				System.out.print(rowData.get(index)+"\t");
			}
			
			System.out.println();
		}
	
		System.out.println(dataSet);
		
	}

}
