package com.stackroute.datamunging.parsing;
import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.processor.AggregateQuery;
import com.stackroute.datamunging.processor.GroupByQuery;
import com.stackroute.datamunging.processor.QueryExecutor;
import com.stackroute.datamunging.processor.SimpleQuery;

public class Query {
	public DataCarrier processorSelection(String queryString) throws Exception {
		DataCarrier dataset = new DataCarrier();
		QueryParser queryParser = new QueryParser(queryString);
		QueryParameter queryParameter = queryParser.querySegregator(queryString);
		QueryExecutor queryExecutor;
		if (queryParameter.isHasAggregate()) {
			queryExecutor = new AggregateQuery();
			dataset = queryExecutor.executeQuery(queryParameter);
		} else if (queryParameter.isHasGroupBy() || queryParameter.isHasOrderBy()) {
			queryExecutor = new GroupByQuery();
			dataset = queryExecutor.executeQuery(queryParameter);
		} else {
			queryExecutor = new SimpleQuery();
			dataset = queryExecutor.executeQuery(queryParameter);
		}
		return dataset;
	}
}



///give same name for instances(query executor instance)