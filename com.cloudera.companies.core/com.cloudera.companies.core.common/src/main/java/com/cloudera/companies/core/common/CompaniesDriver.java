package com.cloudera.companies.core.common;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ShutdownHookManager;
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

	private volatile int exitValue = RETURN_FAILURE_RUNTIME;

	public abstract int prepare(String[] args) throws Exception;

	public abstract int validate() throws Exception;

	public abstract int execute() throws Exception;

	public abstract int cleanup() throws Exception;

	public abstract int shutdown() throws Exception;

	@Override
	public int run(String[] args) {

		long timeTotal = System.currentTimeMillis();
		long timeExecute = -1;

		ShutdownHookManager.get().addShutdownHook(new Runnable() {
			@Override
			public void run() {
				try {
					if (exitValue == RETURN_SUCCESS) {
						shutdown();
					}
				} catch (Exception exception) {
					if (log.isErrorEnabled()) {
						log.error("Exception raised executing shutdown handler", exception);
					}
				}
			}
		}, RunJar.SHUTDOWN_HOOK_PRIORITY + 1);

		if (CompaniesDriver.class.getResource("/" + CONF_SETTINGS) != null) {
			getConf().addResource(CONF_SETTINGS);
		}

		if (log.isDebugEnabled() && getConf() != null) {
			log.debug("Driver [" + this.getClass().getCanonicalName() + "] initialised with configuration properties:");
			for (Entry<String, String> entry : getConf())
				if (log.isDebugEnabled())
					log.debug("\t" + entry.getKey() + "=" + entry.getValue());
		}

		try {
			if ((exitValue = prepare(args)) == RETURN_SUCCESS && (exitValue = validate()) == RETURN_SUCCESS) {
				timeExecute = System.currentTimeMillis();
				exitValue = execute();
				timeExecute = System.currentTimeMillis() - timeExecute;
			}
		} catch (Exception exception) {
			if (log.isErrorEnabled()) {
				log.error("Exception raised executing runtime pipeline handlers", exception);
			}
		} finally {
			try {
				if (exitValue == RETURN_SUCCESS) {
					exitValue = cleanup();
				} else {
					cleanup();
				}
			} catch (Exception exception) {
				if (log.isErrorEnabled()) {
					log.error("Exception raised executing cleanup handler", exception);
				}
			}
		}

		timeTotal = System.currentTimeMillis() - timeTotal;

		if (exitValue == RETURN_SUCCESS) {
			if (log.isInfoEnabled()) {
				log.info("Driver [" + this.getClass().getCanonicalName() + "] executed successfully in [" + timeExecute
						+ "] ms and total time [" + timeTotal + "] ms");
			}
		} else {
			if (log.isInfoEnabled()) {
				log.info("Driver [" + this.getClass().getCanonicalName() + "] failed in total time [" + timeTotal
						+ "] ms");
			}
		}

		return exitValue;
	}

}
