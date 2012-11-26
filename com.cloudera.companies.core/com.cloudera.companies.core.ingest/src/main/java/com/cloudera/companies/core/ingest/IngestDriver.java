package com.cloudera.companies.core.ingest;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.common.CompaniesDriverSuccess;
import com.cloudera.companies.core.ingest.seq.IngestSeqDriver;
import com.cloudera.companies.core.ingest.zip.IngestZipDriver;

public class IngestDriver extends CompaniesDriverSuccess {

	private static Logger log = LoggerFactory.getLogger(IngestDriver.class);

	private String localInputDirPathZip;
	private String hdfsOutputDirPathZip;
	private String hdfsOutputDirPathSeq;

	private static AtomicBoolean isComplete = new AtomicBoolean(false);

	public IngestDriver() {
		super();
	}

	public IngestDriver(Configuration conf) {
		super(conf);
	}

	@Override
	public int prepare(String[] args) throws Exception {

		isComplete.set(false);

		if (args == null || args.length != 3) {
			if (log.isErrorEnabled()) {
				log.error("Usage: " + IngestDriver.class.getSimpleName()
						+ " [generic options] <local-input-dir-zip> <hdfs-output-dir-zip> <hdfs-output-dir-seq>");
				ByteArrayOutputStream byteArrayPrintStream = new ByteArrayOutputStream();
				PrintStream printStream = new PrintStream(byteArrayPrintStream);
				ToolRunner.printGenericCommandUsage(printStream);
				log.error(byteArrayPrintStream.toString());
				printStream.close();
			}
			return CompaniesDriver.RETURN_FAILURE_MISSING_ARGS;
		}

		localInputDirPathZip = args[0];
		hdfsOutputDirPathZip = args[1];
		hdfsOutputDirPathSeq = args[2];

		return RETURN_SUCCESS;
	}

	@Override
	public int execute() throws Exception {

		int returnValue = RETURN_FAILURE_RUNTIME;
		IngestZipDriver ingestZipDriver = new IngestZipDriver(getConf());
		IngestSeqDriver ingestSeqDriver = new IngestSeqDriver(getConf());
		if ((returnValue = ingestZipDriver.run(new String[] { localInputDirPathZip, hdfsOutputDirPathZip })) == RETURN_SUCCESS) {
			returnValue = ingestSeqDriver.run(new String[] { hdfsOutputDirPathZip, hdfsOutputDirPathSeq });
		}
		isComplete.set(true);

		importCounters(ingestZipDriver.getCounters());
		importCounters(ingestSeqDriver.getCounters());

		return returnValue;
	}

	@Override
	public int shutdown() {

		if (!isComplete.get()) {
			if (log.isErrorEnabled()) {
				log.error("Halting before completion, ingest may be only partially complete (and may contiue asyncrhonously)");
			}
		}

		return RETURN_SUCCESS;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new IngestDriver(), args));
	}
}
