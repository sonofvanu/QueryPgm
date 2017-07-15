package Query.QueryProctor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class FileHandler {

	QueryParser queryparser = new QueryParser();
	String alldata[], str, finaldata[], withoutheader[], conditions[], hdr, headerdata[], columnorder, columnsreceived,
			columns[], query;
	private ColumnNames columnamess = new ColumnNames();
	Map<String, Integer> headermap = new HashMap<>();
	Map<Integer, String> rowdatamap = new TreeMap<>();

	////// ---Fetching of data from the csv file and trying to put the row data
	////// in an array----///

	public String[] fetchingRowData(String filename) throws Exception {
		String str;
		FileReader f = new FileReader("g:\\" + queryparser.filepath);
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
		this.checkingHeaderInColumn(queryparser.query);
		this.ColumnDataProcessor(columns, rowdatamap, headermap);
		this.columnsAndWhereCondition(queryparser, headermap);
		this.whereConditionProcessing(queryparser, headermap);
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
			headermap.put(headerdata[i], i);
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

			for (String string : finaldata) {
				System.out.println(string.replace("\"", "").replace(",", " "));
			}
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
							System.out.println(temp[position].replaceAll("^\"|\"$", ""));

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

			for (int i = 0; i < lengthcount; i++) {
				indexofhead[i] = i;
			}
			for (String in : rowdata.values()) {
				databeforeparsing.add(in);
			}

			int rowdatalength = databeforeparsing.size();

			String storedata[] = new String[rowdatalength];

			for (int j = 0; j < rowdatalength; j++) {
				storedata[j] = databeforeparsing.get(j);
				String datasftersplit[] = storedata[j].split(",");

				StringBuffer stringbuffer = null;
				stringbuffer = new StringBuffer();
				for (int k = 0; k < lengthcount; k++) {
					stringbuffer.append(datasftersplit[indexofhead[k]] + ",");
				}
				newMap.put(j, stringbuffer.toString());
			}

		}
System.out.println(newMap);
		return newMap;

	}

	////// ---where condition based displaying of data only for =
	////// operator----/////
	public Map<Integer, String> columnsAndWhereCondition(QueryParser queryparser, Map<String, Integer> header) {
		Map<Integer, String> rowwsiedata = new HashMap<>();
		try {
			BufferedReader bufferedreader = new BufferedReader(new FileReader(queryparser.filepath));
			int indexpoint = 0, columncounter = queryparser.getColumnnamelist().size();
			String stringreader = bufferedreader.readLine();
			while (stringreader != null) {
				String[] splittedstring = stringreader.split(",");
				StringBuffer stringbuffer = new StringBuffer();
				for (int i = 0; i < columncounter; i++) {
					stringbuffer.append(splittedstring[header.get(queryparser.getColumnnamelist().get(i))] + ",");
					rowwsiedata.put(indexpoint, stringbuffer.toString());
				}
				indexpoint++;
				stringreader = bufferedreader.readLine();
			}
		} catch (Exception e) {

		}
		System.out.println(rowwsiedata);
		return rowwsiedata;
	}

	////// ---where condition based displaying of data only for =
	////// operator----/////
	public Map<Integer, String> whereConditionProcessing(QueryParser queryparser, Map<String, Integer> header) {
		Map<Integer, String> rowwisedata = new HashMap<>();
		try {
			BufferedReader bufferedreader = new BufferedReader(new FileReader(queryparser.filepath));
			String conditioncolmnname = queryparser.relationalExpressionProcessing(queryparser.relationalquery).get(0)
					.getColumn();
			String conditionvalue = queryparser.relationalExpressionProcessing(queryparser.relationalquery).get(0)
					.getValue();
			int indexpoint = 0, helper = 0;
			int columncount = queryparser.getColumnnamelist().size();
			String string = bufferedreader.readLine();
			if (queryparser.getColumnnamelist().get(0).equals("*")) {
				while (string != null) {
					String afterdatasplit[] = str.split(",");
					if (afterdatasplit[header.get(conditioncolmnname)].equals(conditionvalue)) {
						rowwisedata.put(indexpoint, string);
						helper++;
					}
					indexpoint++;
					string = bufferedreader.readLine();
				}
				System.out.println(rowwisedata);
				return rowwisedata;
			} else {
				while (string != null) {
					String afterdatasplit[] = string.split(",");
					if (afterdatasplit[header.get(conditioncolmnname)].equals(conditionvalue)) {
						StringBuffer stringbuffer = new StringBuffer();
						for (helper = 0; helper < columncount; helper++) {
							stringbuffer
									.append(afterdatasplit[header.get(queryparser.getColumnnamelist().get(helper))]);
							rowwisedata.put(indexpoint, stringbuffer.toString());
						}
					}
					indexpoint++;
					string = bufferedreader.readLine();
				}
			}

		} catch (Exception e) {
		}
		System.out.println(rowwisedata);
		return rowwisedata;
	}

}
