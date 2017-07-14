package Query.QueryProctor;

import java.util.Scanner;

public class QueryInput {
	public static void main(String[] args) {

		QueryParser queryparser = new QueryParser();
		queryparser.inputQuerryArray();

	}

	public String queryGetter() {
		String inputquery;
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter the query to process: ");
		inputquery = scanner.nextLine();
		return inputquery;

	}

}
