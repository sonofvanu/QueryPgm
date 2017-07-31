package com.stackroute.datamunging.processor;

import com.stackroute.datamunging.model.DataCarrier;
import com.stackroute.datamunging.parsing.QueryParameter;

public interface QueryExecutor {

	public DataCarrier executeQuery(QueryParameter queryParameter) throws Exception;
}
