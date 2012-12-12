package com.cloudera.companies.core.common;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompaniesDriver extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(CompaniesDriver.class);

	public static final int RETURN_SUCCESS = 0;
	public static final int RETURN_WARNING_DIRTY_INGEST = 100;
	public static final int RETURN_FAILURE_MISSING_ARGS = 200;
	public static final int RETURN_FAILURE_INVALID_ARGS = 201;
	public static final int RETURN_FAILURE_RUNTIME = 202;

	public static final String CONF_SETTINGS = "companies-site.xml";

	public static final String CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME = "_SUCCESS";

	private static final int FORMAT_TIME_FACTOR = 10;

	private volatile int exitValue = RETURN_FAILURE_RUNTIME;

	private Map<String, Map<Enum<?>, Long>> counters = new LinkedHashMap<String, Map<Enum<?>, Long>>();

	public CompaniesDriver() {
		super();
	}

	public CompaniesDriver(Configuration conf) {
		super(conf);
	}

	public abstract int prepare(String[] args) throws Exception;

	public abstract int validate() throws Exception;

	public abstract int execute() throws Exception;

	public abstract int cleanup() throws Exception;

	public abstract int shutdown() throws Exception;

	public Map<String, Map<Enum<?>, Long>> getCounters() {
		return new LinkedHashMap<String, Map<Enum<?>, Long>>(counters);
	}

	public Map<Enum<?>, Long> getCounters(String group) {
		return counters.get(group) == null ? Collections.<Enum<?>, Long> emptyMap() : new LinkedHashMap<Enum<?>, Long>(
				counters.get(group));
	}

	public Set<String> getCountersGroups() {
		return new HashSet<String>(counters.keySet());
	}

	protected void importCounters(Map<String, Map<Enum<?>, Long>> counters) {
		for (String group : counters.keySet()) {
			importCounters(group, counters.get(group));
		}
	}

	protected void importCounters(String group, Map<Enum<?>, Long> counters) {
		if (this.counters.get(group) == null) {
			this.counters.put(group, new LinkedHashMap<Enum<?>, Long>());
		}
		for (Enum<?> value : counters.keySet()) {
			if (counters.get(value) != null) {
				this.counters.get(group).put(
						value,
						(this.counters.get(group).get(value) == null ? 0 : this.counters.get(group).get(value))
								+ counters.get(value));
			}
		}
	}

	protected void importCounters(String group, Job job, Enum<?>[] values) throws IOException, InterruptedException {
		if (this.counters.get(group) == null) {
			this.counters.put(group, new LinkedHashMap<Enum<?>, Long>());
		}
		Counters counters = job.getCounters();
		for (Enum<?> value : values) {
			if (counters.findCounter(value) != null) {
				this.counters.get(group).put(
						value,
						(this.counters.get(group).get(value) == null ? 0 : this.counters.get(group).get(value))
								+ counters.findCounter(value).getValue());
			}
		}
	}

	private void cleanClounters() {
		counters.clear();
	}

	public Long getCounter(String group, Enum<?> counter) {
		return (counters.get(group) == null || counters.get(group).get(counter) == null) ? null : counters.get(group)
				.get(counter);
	}

	public Long incramentCounter(String group, Enum<?> counter, int incrament) {
		if (this.counters.get(group) == null) {
			this.counters.put(group, new LinkedHashMap<Enum<?>, Long>());
		}
		return counters.get(group).put(counter,
				(counters.get(group).get(counter) == null ? 0 : counters.get(group).get(counter)) + incrament);
	}

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

		cleanClounters();
		
		if (log.isDebugEnabled() && getConf() != null) {
			log.debug("Driver [" + this.getClass().getCanonicalName() + "] initialised with configuration properties:");
			for (Entry<String, String> entry : getConf())
				if (log.isDebugEnabled())
					log.debug("\t" + entry.getKey() + "=" + entry.getValue());
		}

		timeExecute = System.currentTimeMillis();
		try {
			if ((exitValue = prepare(args)) == RETURN_SUCCESS && (exitValue = validate()) == RETURN_SUCCESS) {
				exitValue = execute();
			}
		} catch (Exception exception) {
			if (log.isErrorEnabled()) {
				log.error("Exception raised executing runtime pipeline handlers", exception);
			}
		} finally {
			timeExecute = System.currentTimeMillis() - timeExecute;
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

		if (log.isInfoEnabled()) {
			log.info("Driver [" + this.getClass().getCanonicalName() + "] counters:");

			for (String group : getCountersGroups()) {
				Map<Enum<?>, Long> counters = getCounters(group);
				for (Enum<?> counter : counters.keySet()) {
					log.info("\t" + group + "." + counter.toString() + "=" + counters.get(counter));
				}
			}
		}

		if (log.isInfoEnabled()) {
			log.info("Driver [" + this.getClass().getCanonicalName() + "] "
					+ (exitValue == RETURN_SUCCESS ? "sucessful" : "failed") + " with exit value [" + exitValue
					+ "] in " + formatTime(timeExecute) + " and total time " + formatTime(timeTotal));
		}

		return exitValue;
	}

	private static String formatTime(long time) {
		StringBuilder string = new StringBuilder();
		int factor;
		String unit;
		if (time < 0) {
			time = 0;
			factor = 1;
			unit = "ms";
		} else if (time < FORMAT_TIME_FACTOR * 1000) {
			factor = 1;
			unit = "ms";
		} else if (time < FORMAT_TIME_FACTOR * 1000 * 60) {
			factor = 1000;
			unit = "sec";
		} else if (time < FORMAT_TIME_FACTOR * 1000 * 60 * 60) {
			factor = 1000 * 60;
			unit = "min";
		} else {
			factor = 1000 * 60 * 60;
			unit = "hour";
		}
		string.append("[");
		string.append(time / factor);
		string.append("] ");
		string.append(unit);
		return string.toString();
	}
}
