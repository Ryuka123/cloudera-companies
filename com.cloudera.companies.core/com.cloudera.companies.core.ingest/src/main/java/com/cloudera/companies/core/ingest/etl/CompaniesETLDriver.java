package com.cloudera.companies.core.ingest.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.companies.core.common.CompaniesDriver;

public class CompaniesETLDriver extends CompaniesDriver {

	public static enum RecordCounter {
		VALID, MALFORMED, MALFORMED_KEY, MALFORMED_DUPLICATE
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(CompaniesETLDriver.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CompaniesETLDriver(), args));
	}
}
