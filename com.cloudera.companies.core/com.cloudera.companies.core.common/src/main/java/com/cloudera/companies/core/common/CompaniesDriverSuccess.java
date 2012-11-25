package com.cloudera.companies.core.common;

import org.apache.hadoop.conf.Configuration;

public class CompaniesDriverSuccess extends CompaniesDriver {

	public CompaniesDriverSuccess() {
		super();
	}

	public CompaniesDriverSuccess(Configuration conf) {
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
