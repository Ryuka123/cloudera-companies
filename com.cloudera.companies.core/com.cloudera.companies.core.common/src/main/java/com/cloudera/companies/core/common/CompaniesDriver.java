package com.cloudera.companies.core.common;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompaniesDriver extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(CompaniesDriver.class);

	@Override
	public void setConf(Configuration conf) {
		if (log.isDebugEnabled() && conf != null) {
			log.debug("Driver [" + this.getClass().getCanonicalName() + "] initialised with configuration properties:");
			for (Entry<String, String> entry : conf)
				if (log.isDebugEnabled())
					log.debug("\t" + entry.getKey() + "=" + entry.getValue());
		}
		super.setConf(conf);
	}

}
