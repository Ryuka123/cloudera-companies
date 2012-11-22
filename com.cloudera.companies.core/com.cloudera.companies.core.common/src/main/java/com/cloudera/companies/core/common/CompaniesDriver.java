package com.cloudera.companies.core.common;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompaniesDriver extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(CompaniesDriver.class);

	public static final int RETURN_SUCCESS = 0;
	public static final int RETURN_FAILURE_MISSING_ARGS = 1;
	public static final int RETURN_FAILURE_INVALID_ARGS = 2;
	public static final int RETURN_FAILURE_RUNTIME = 3;

	public static final String CONF_SETTINGS = "companies-site.xml";

	public static final String CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME = "_SUCCESS";

	public abstract int runCompaniesDriver(String[] args) throws Exception;

	@Override
	public int run(String[] args) throws Exception {

		if (CompaniesDriver.class.getResource("/" + CONF_SETTINGS) != null) {
			getConf().addResource(CONF_SETTINGS);
		}

		if (log.isDebugEnabled() && getConf() != null) {
			log.debug("Driver [" + this.getClass().getCanonicalName() + "] initialised with configuration properties:");
			for (Entry<String, String> entry : getConf())
				if (log.isDebugEnabled())
					log.debug("\t" + entry.getKey() + "=" + entry.getValue());
		}

		return runCompaniesDriver(args);
	}

}
