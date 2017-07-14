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
	private ColumnNames columnames = new ColumnNames();
	Map<String, Integer> headermap = new HashMap<>();
	Map<Integer, String> rowdatamap = new TreeMap<>();

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
		return finaldata;
	}

	public String getColumnorder() {
		return columnorder;
	}

	public void setColumnorder(String columnorder) {
		this.columnorder = columnorder;
	}

	public Map<String, Integer> gettingHeaderMap() {
		headerdata = finaldata[0].split(",");

		int length = headerdata.length;
		for (int i = 0; i < length; i++) {
			headermap.put(headerdata[i], i);
		}

		return headermap;
	}

	public Map<Integer, String> gettingRowDataMap() {

		int length = finaldata.length;
		for (int i = 0; i < length; i++) {
			rowdatamap.put(i, finaldata[i]);
		}

		return rowdatamap;
	}

	public String[] columnSeparator() {
		String[] separateddata = query.split(" ");
		columnsreceived = separateddata[1].trim();
		columns = columnsreceived.split(",");
		return columns;

	}

	public FileHandler checkingHeaderInColumn(String query) {

		String[] separateddata = query.split(" ");
		columnsreceived = separateddata[1].trim();
		columns = columnsreceived.split(",");
		int position;
		if (columns.length == 1 || columnsreceived.contains("*")) {

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

	public Map<Integer, String> ColumnDataProcessor(String[] columns, Map<Integer, String> rowdata,
			Map<String, Integer> header) {
		Map<Integer, String> newMap = new HashMap<>();
		int lengthcount = columns.length;
		List<String> databeforeparsing = new ArrayList<>();
		List<String> dataafterparsing = new ArrayList<>();
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

		return newMap;

	}

}
