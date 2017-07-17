package Query.QueryProctor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class FileHandler {

	QueryParser queryparser = new QueryParser();
	public String alldata[], str, finaldata[], withoutheader[], conditions[], hdr, headerdata[], columnorder,
			columnsreceived, columns[], query, relationalquery;
	public String filename;
	private ColumnNames columnamess = new ColumnNames();
	public Map<Integer, String> singlewheremap = new HashMap();
	public Map<String, Integer> headermap = new HashMap<>();
	public Map<Integer, String> resultantmap = new HashMap<>();
	public Map<Integer, String> middlemap = new HashMap<>();
	public Map<Integer, String> rowdatamap = new TreeMap<>();
	public List<RelationalConditions> relationalList = new ArrayList<>();
	////// ---Fetching of data from the csv file and trying to put the row data
	////// in an array----///

	public String[] fetchingRowData(String filename, String query, String relationalquery, List relationallist)
			throws Exception {
		this.filename = filename;
		this.query = query;
		this.relationalquery = relationalquery;
		this.relationalList = relationallist;
		String str;
		FileReader f = new FileReader("g:\\" + filename);
		BufferedReader br = new BufferedReader(f);
		List<String> list = new ArrayList<String>();
		while ((str = br.readLine()) != null) {

			list.add(str);
		}
		String[] stringArr = list.toArray(new String[0]);

		finaldata = Arrays.copyOf(stringArr, stringArr.length - 1);

		this.gettingHeaderMap();
		this.gettingRowDataMap();
		this.columnSeparator();
		this.checkingHeaderInColumn(query);
		this.ColumnDataProcessor(columns, rowdatamap, headermap);
		this.whereQueryProcessing(queryparser, headermap);
		//this.multiWhereProcessor(queryparser, headermap);
		return finaldata;
	}

	public String getColumnorder() {
		return columnorder;
	}

	public void setColumnorder(String columnorder) {
		this.columnorder = columnorder;
	}

	///// ----storing the splitted first header into a map of header name and
	///// indexpoint----////
	public Map<String, Integer> gettingHeaderMap() {
		headerdata = finaldata[0].split(",");

		int length = headerdata.length;
		for (int i = 0; i < length; i++) {
			headermap.put(headerdata[i].replaceAll("^\"|\"$", ""), i);
		}

		return headermap;
	}

	////// ----storing the splitted data i.e., each row into a map with their
	////// row number----/////

	public Map<Integer, String> gettingRowDataMap() {

		int length = finaldata.length;
		for (int i = 0; i < length; i++) {
			rowdatamap.put(i, finaldata[i]);
		}

		return rowdatamap;
	}

	//// ---Separated column names in the select query---//////
	public String[] columnSeparator() {
		String[] separateddata = query.split(" ");
		columnsreceived = separateddata[1].trim();
		columns = columnsreceived.split(",");
		return columns;

	}

	///// ---- displaying of all the column based data----////
	public FileHandler checkingHeaderInColumn(String query) {

		String[] separateddata = query.split(" ");
		columnsreceived = separateddata[1].trim();
		columns = columnsreceived.split(",");
		int position;
		if (columns.length == 1 || columns.equals("*")) {

		} else {
			Set entrySet = headermap.entrySet();

			Iterator it = entrySet.iterator();

			while (it.hasNext()) {
				for (int i = 0; i < columns.length; i++) {
					Map.Entry me = (Map.Entry) it.next();

					String value = (String) me.getKey();
					if (value.contains(columns[i])) {
						position = (int) me.getValue();

						for (int s = 0; s < finaldata.length; s++) {
							String[] temp = finaldata[s].split(",");

						}

					}

				}

			}
		}
		return this;
	}

	//// -------------column where conditioning---//////////////
	public Map<Integer, String> ColumnDataProcessor(String[] columns, Map<Integer, String> rowdata,
			Map<String, Integer> header) {
		Map<Integer, String> newMap = new HashMap<>();
		int lengthcount = columns.length;
		List<String> databeforeparsing = new ArrayList<>();
		int indexofhead[] = new int[lengthcount];
		if (columns[0].contains("*")) {
			this.checkingHeaderInColumn(query);
		} else {

			int definedlength = headerdata.length;

			for (int i = 0; i < lengthcount; i++) {
				for (int j = 0; j < definedlength; j++) {
					if (headerdata[j].contains(columns[i])) {
						indexofhead[i] = j;

					}
				}

			}

			for (String in : rowdata.values()) {
				databeforeparsing.add(in);
			}

//			for (Integer i : indexofhead) {
//				System.out.println(i);
//			}
			int rowdatalength = databeforeparsing.size();

			String storedata[] = new String[rowdatalength];

			for (int r = 0; r < rowdatalength; r++) {
				storedata[r] = databeforeparsing.get(r);
				String datasftersplit[] = storedata[r].split(",");

				StringBuffer stringbuffer = null;
				stringbuffer = new StringBuffer();
				for (int k = 0; k < lengthcount; k++) {
					stringbuffer.append(datasftersplit[indexofhead[k]] + ",");
				}
				newMap.put(r, stringbuffer.toString());
			}

		}
		return newMap;

	}

	public Map<Integer, String> whereQueryProcessing(QueryParser queryparser, Map<String, Integer> header)
			throws IOException {
		Map<Integer, String> rowwisedata = new HashMap<>();
		
		List<String> middlelist=new ArrayList<>();
		List<String> resultantlist=new ArrayList<>();

		BufferedReader bufferedreader = null;
		String columnname = "", values = "", operator = "";
		int lengthcount = columns.length;
		int[] indexofhead = new int[lengthcount];
		int indexcount = 0, columncount = 0, temp = 0, headerposition = 0;
		String string = "";
		int definedlength = headerdata.length;
		try {
			Iterator iterator = relationalList.iterator();
			while (iterator.hasNext()) {
				RelationalConditions rc = (RelationalConditions) iterator.next();
				columnname = rc.getColumn();

				for (int count = 0; count < definedlength; count++) {
					String hdr = headerdata[count].replaceAll("^\"|\"$", "");

					if (hdr.contains(columnname)) {

						headerposition = count;

					}
				}
				values = rc.getValue();
				operator = rc.getOperator();
			
			bufferedreader = new BufferedReader(new FileReader("g:\\" + filename));

			columncount = columns.length;
			string = bufferedreader.readLine();
			string = bufferedreader.readLine();
			for (int i = 0; i < lengthcount; i++) {
				for (int j = 0; j < definedlength; j++) {
					if (headerdata[j].contains(columns[i])) {
						indexofhead[i] = j;

					}
				}

			}
			if (columns[0].equals("*")) {
				while (string != null) {
					if (operator.equals("="))
					{
						String[] afterdatasplit = string.split(",");

						if (afterdatasplit[headerposition].equals(values)) {
							rowwisedata.put(indexcount, string);
							temp++;
						}
						indexcount++;
						string = bufferedreader.readLine();
					}
				}
				
				return rowwisedata;
			} else {
				while (string != null) {
					String afterdatasplit[] = string.split(",");
					for (int i = 0; i < lengthcount; i++) {
						for (int j = 0; j < definedlength; j++) {
							if (headerdata[j].contains(columns[i])) {
								indexofhead[i] = j;

							}
						}

					}
					if (operator.equals("=")) {
						long residence = Math.max(
								Long.parseLong(afterdatasplit[headerposition].replaceAll("^\"|\"$", "")),
								Long.parseLong(values));
						if (Long.parseLong(afterdatasplit[headerposition].replaceAll("^\"|\"$", "")) == Long
								.parseLong(values)) {
							StringBuffer stringbuffer = new StringBuffer();

							for (int k = 0; k < lengthcount; k++) {
								for (temp = 0; temp < columncount - 1; temp++) {
									stringbuffer.append(afterdatasplit[indexofhead[k]] + ",");
									String stringg = stringbuffer.toString();

									rowwisedata.put(indexcount, stringg.substring(0, stringg.length() - 1));

								}

							}

						}
						indexcount++;
						string = bufferedreader.readLine();
					}

					else if (operator.equals(">")) {
						long residence = Math.max(
								Long.parseLong(afterdatasplit[headerposition].replaceAll("^\"|\"$", "")),
								Long.parseLong(values));
						if (Long.parseLong(
								afterdatasplit[headerposition].replaceAll("^\"|\"$", "")) > (Long.parseLong(values))) {
							StringBuffer stringbuffer = new StringBuffer();

							for (int k = 0; k < lengthcount; k++) {
								for (temp = 0; temp < columncount - 1; temp++) {
									stringbuffer.append(afterdatasplit[indexofhead[k]] + ",");
									String stringg = stringbuffer.toString();

									rowwisedata.put(indexcount, stringg.substring(0, stringg.length() - 1));

								}

							}

						}
						indexcount++;
						string = bufferedreader.readLine();

					}

					else if (operator.equals(">=")) {
						long residence = Math.max(
								Long.parseLong(afterdatasplit[headerposition].replaceAll("^\"|\"$", "")),
								Long.parseLong(values));
						if (Long.parseLong(
								afterdatasplit[headerposition].replaceAll("^\"|\"$", "")) >= (Long.parseLong(values))) {
							StringBuffer stringbuffer = new StringBuffer();

							for (int k = 0; k < lengthcount; k++) {
								for (temp = 0; temp < columncount - 1; temp++) {
									stringbuffer.append(afterdatasplit[indexofhead[k]] + ",");
									String stringg = stringbuffer.toString();

									rowwisedata.put(indexcount, stringg.substring(0, stringg.length() - 1));

								}

							}

						}
						indexcount++;
						string = bufferedreader.readLine();
					} else if (operator.equals("<")) {
						long residence = Math.max(
								Long.parseLong(afterdatasplit[headerposition].replaceAll("^\"|\"$", "")),
								Long.parseLong(values));
						if (Long.parseLong(
								afterdatasplit[headerposition].replaceAll("^\"|\"$", "")) < (Long.parseLong(values))) {
							StringBuffer stringbuffer = new StringBuffer();

							for (int k = 0; k < lengthcount; k++) {
								for (temp = 0; temp < columncount - 1; temp++) {
									stringbuffer.append(afterdatasplit[indexofhead[k]] + ",");
									String stringg = stringbuffer.toString();

									rowwisedata.put(indexcount, stringg.substring(0, stringg.length() - 1));

								}

							}

						}
						indexcount++;
						string = bufferedreader.readLine();
					} else if (operator.equals("<=")) {
						long residence = Math.max(
								Long.parseLong(afterdatasplit[headerposition].replaceAll("^\"|\"$", "")),
								Long.parseLong(values));
						if (Long.parseLong(
								afterdatasplit[headerposition].replaceAll("^\"|\"$", "")) <= (Long.parseLong(values))) {
							StringBuffer stringbuffer = new StringBuffer();

							for (int k = 0; k < lengthcount; k++) {
								for (temp = 0; temp < columncount - 1; temp++) {
									stringbuffer.append(afterdatasplit[indexofhead[k]] + ",");
									String stringg = stringbuffer.toString();

									rowwisedata.put(indexcount, stringg.substring(0, stringg.length() - 1));

								}

							}

						}
						indexcount++;
						string = bufferedreader.readLine();
					}

				}
			}
			
			List<String> rowiselist =new ArrayList<>(rowwisedata.values());
			rowwisedata.clear();
			if(middlelist.isEmpty())
			{
				Collections.copy(middlelist, rowiselist);
				rowiselist.clear();
			}
			else{
				
				Iterator iteratorlist=queryparser.logicalOperator.iterator();
				while(iteratorlist.hasNext())
				{
					if(((String) iteratorlist.next()).contains("and"))
					{
				resultantlist=this.intersection(middlelist, rowiselist);
				middlelist.clear();
				rowiselist.clear();
				Collections.copy(middlelist, resultantlist);
					}
					else if(((String)iteratorlist.next()).contains("or"))
					{
					resultantlist=this.union(middlelist, rowiselist);
					middlelist.clear();
					rowiselist.clear();
					Collections.copy(middlelist,resultantlist);
					
					}
					
				}
				
			}
			
			Iterator listtomap=resultantlist.iterator();
			while(listtomap.hasNext())
			{
				for(int i=0;i<resultantlist.size();i++)
				{
					rowwisedata.put(i, (String) listtomap.next());
				}
				
			}
			

						}

		} catch (Exception e) {
			// redirect them to string columns;
		}
		
System.out.println(rowwisedata);

		return rowwisedata;
	}

//	public Map<Integer, String> multiWhereProcessor(QueryParser queryparser, Map<String, Integer> headermap) {
//		Map<Integer, String> rowdata = new HashMap<>();
//		BufferedReader reader = null;
//		String columnname = "", values = "", operator = "", columnname1 = "", values1 = "", operator1 = "";
//		String colname = "";
//		String valu = "";
//		int index = 0, h = 0;
//		int indexcount = 0, columncount = 0, temp = 0, headerposition = 0;
//		int definedlength = headerdata.length;
//		int colcount = 0;
//		String str = "";
//		try {
//			Iterator iterator = relationalList.iterator();
//			while (iterator.hasNext()) {
//				RelationalConditions rc = (RelationalConditions) iterator.next();
//				columnname = rc.getColumn();
//				values = rc.getValue();
//				operator = rc.getOperator();
//				RelationalConditions rc1 = (RelationalConditions) iterator.next();
//				columnname1 = rc1.getColumn();
//				values1 = rc1.getValue();
//				operator1 = rc1.getOperator();
//
//				for (int count = 0; count < definedlength; count++) {
//					String hdr = headerdata[count].replaceAll("^\"|\"$", "");
//					// System.out.println(columns[count]);
//					if (hdr.contains(columnname)) {
//
//						headerposition = count;
//						System.out.println(headerposition);
//					}
//				}
//
//			}
//			reader = new BufferedReader(new FileReader("g:\\" + filename));
//
//			colcount = columns.length;
//			str = reader.readLine();
//			str = reader.readLine();
//			if (columns[0].equals("*")) {
//
//				while (str != null) {
//					if (operator.equals("=")) {
//						String aftersplit[] = str.split(",");
//						if (aftersplit[headerposition].equals(valu)) {
//							rowdata.put(index, str);
//							h++;
//
//						}
//						index++;
//						str = reader.readLine();
//					}
//				}
//
//				return rowdata;
//			} else {
//				while (str != null) {
//					String op1 = operator;
//					String op2 = operator1;
//					// out switch op1
//					switch (op1) {
//					case "=":
//						switch (op2) {// op1 case1 inner case op2
//						case "=":// op2 case 1 start
//							if (queryparser.logicalOperator.get(0).equals("and")) {
//								String aftersplit[] = str.split(",");
//								if (aftersplit[headermap.get(columnname)].equals(values)
//										&& aftersplit[headermap.get(columnname1)].equals(values1)) {
//									rowdata.put(index, str);
//									h++;
//
//								}
//								index++;
//								str = reader.readLine();
//							} else {
//
//							}
//
//							break;
//						case ">":
//							if (queryparser.logicalOperator.get(0).equals("and")) {
//								String aftersplit[] = str.split(",");
//								if (aftersplit[headermap.get(columnname)].equals(values)
//										&& (aftersplit[headermap.get(columnname1)].compareTo(values1)) > 0) {
//
//									StringBuffer sb = new StringBuffer();
//									for (h = 0; h < colcount; h++) {
//										sb.append(aftersplit[headermap.get(columns[h])] + ",");
//										String st = sb.toString();
//										rowdata.put(index, st.substring(0, st.length() - 1));
//									}
//									h++;
//
//								}
//								index++;
//								str = reader.readLine();
//							} else {
//
//							}
//							break;
//						case "<":
//							break;
//						case ">=":
//							break;
//						case "<+":
//							break;
//						case "!=":
//							break;
//						}
//						break;// op2 case 1 end
//					case ">":
//						break;
//					case "<":
//						break;
//					case ">=":// op1 case 4 start
//						switch (op2) {
//						case "=":
//							if (queryparser.logicalOperator.get(0).equals("and")) {
//								String aftersplit[] = str.split(",");
//								if (aftersplit[headermap.get(columnname)].equals(values)
//										&& aftersplit[headermap.get(columnname1)].equals(values1)) {
//									rowdata.put(index, str);
//									h++;
//
//								}
//								index++;
//								str = reader.readLine();
//							} else {
//
//							}
//
//							break;
//						case ">":
//							if (queryparser.logicalOperator.get(0).equals("and")) {
//								String aftersplit[] = str.split(",");
//								if (aftersplit[headermap.get(columnname)].equals(values)
//										&& (aftersplit[headermap.get(columnname1)].compareTo(values1)) > 0) {
//
//									StringBuffer sb = new StringBuffer();
//									for (h = 0; h < colcount; h++) {
//										sb.append(aftersplit[headermap.get(columns[h])] + ",");
//										String st = sb.toString();
//										rowdata.put(index, st.substring(0, st.length() - 1));
//									}
//									h++;
//
//								}
//								index++;
//								str = reader.readLine();
//							} else {
//
//							}
//							break;
//						case "<":
//							break;
//						case ">=":
//							break;
//						case "<=":
//							if (queryparser.logicalOperator.get(0).equals("and")) {
//								String aftersplit[] = str.split(",");
//								String left1 = aftersplit[headermap.get(columnname)];
//								String left2 = aftersplit[headermap.get(columnname1)];
//								String valu1 = values;
//								String valu2 = values1;
//								int res1 = left1.compareTo(valu1);
//								int res2 = left2.compareTo(valu2);
//								if ((res1 > 0 || res1 == 0) && (res2 < 0 || res2 == 0)) {
//
//									StringBuffer sb = new StringBuffer();
//									for (h = 0; h < colcount; h++) {
//										sb.append(aftersplit[headermap.get(columns[h])] + ",");
//										String st = sb.toString();
//										rowdata.put(index, st.substring(0, st.length() - 1));
//									}
//									h++;
//
//								}
//								index++;
//								str = reader.readLine();
//							} else {
//
//							}
//
//							break;
//						case "!=":
//							break;
//						}
//						break;
//					case "<+":
//						break;
//					case "!=":
//						break;
//
//					}
//
//				}
//			}
//
//		} catch (IOException e) {
//		}
//		System.out.println("after where processor" + rowdata);
//		return rowdata;
//	}
	public <T> List<T> union(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<T>();

        set.addAll(list1);
        set.addAll(list2);

        return new ArrayList<T>(set);
    }

    public <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();

        for (T t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }

}