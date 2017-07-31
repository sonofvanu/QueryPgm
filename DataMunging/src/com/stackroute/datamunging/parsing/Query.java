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
		if (queryParameter.isHasAggregate()) {
			QueryExecutor queryExecutorAggregate = new AggregateQuery();
			dataset = queryExecutorAggregate.executeQuery(queryParameter);
		} else if (queryParameter.isHasGroupBy() || queryParameter.isHasOrderBy()) {
			QueryExecutor queryExecutorGroup = new GroupByQuery();
			dataset = queryExecutorGroup.executeQuery(queryParameter);
		} else {
			QueryExecutor queryExecutorSimple = new SimpleQuery();
			dataset = queryExecutorSimple.executeQuery(queryParameter);
		}
		return dataset;
	}
}
