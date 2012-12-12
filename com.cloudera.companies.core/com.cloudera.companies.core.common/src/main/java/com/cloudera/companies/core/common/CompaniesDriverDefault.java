package com.cloudera.companies.core.common;

import org.apache.hadoop.conf.Configuration;

public class CompaniesDriverDefault extends CompaniesDriver {

	public CompaniesDriverDefault() {
		super();
	}

	public CompaniesDriverDefault(Configuration conf) {
		super(conf);
	}

	@Override
	public int prepare(String[] args) throws Exception {
		return RETURN_SUCCESS;
	}

	@Override
	public int validate() throws Exception {
		return RETURN_SUCCESS;
	}

	@Override
	public int execute() throws Exception {
		return RETURN_SUCCESS;
	}

	@Override
	public int cleanup() throws Exception {
		return RETURN_SUCCESS;
	}

	@Override
	public int shutdown() throws Exception {
		return RETURN_SUCCESS;
	}

}
