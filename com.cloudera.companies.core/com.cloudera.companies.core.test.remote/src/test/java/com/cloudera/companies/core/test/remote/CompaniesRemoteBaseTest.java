package com.cloudera.companies.core.test.remote;

import java.io.File;
import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import com.cloudera.companies.core.test.CompaniesBaseTestCase;

public abstract class CompaniesRemoteBaseTest extends CompaniesBaseTestCase {

	public static final String PATH_HADOOP_BIN = FileUtils
			.listFiles(new File(CompaniesBaseTestCase.getPathLocal("target/test-runtime")),
					new WildcardFileFilter("hadoop"), TrueFileFilter.TRUE).iterator().next().getAbsolutePath();
	public static final String PATH_ASSEMBLY_INGEST_JAR = FileUtils
			.listFiles(new File(CompaniesBaseTestCase.getPathLocal("target/test-assembly")),
					new WildcardFileFilter("com.cloudera.companies.core.ingest-*-hadoop-job.jar"), TrueFileFilter.TRUE)
			.iterator().next().getAbsolutePath();

	public int execute(String bin, String... args) throws ExecuteException, IOException {
		return getExecutor().execute(getCommandLine(bin, args));
	}

	public CommandLine getCommandLine(String bin, String... args) {
		CommandLine commandLine = new CommandLine(bin);
		for (String arg : args) {
			commandLine.addArgument(arg);
		}
		return commandLine;
	}

	public Executor getExecutor() {
		Executor executor = new DefaultExecutor();
		executor.setExitValue(0);
		return executor;
	}
}