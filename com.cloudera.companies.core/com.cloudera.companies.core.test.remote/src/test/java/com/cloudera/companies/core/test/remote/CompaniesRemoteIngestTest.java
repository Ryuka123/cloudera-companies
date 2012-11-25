package com.cloudera.companies.core.test.remote;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.companies.core.ingest.IngestDriver;

public class CompaniesRemoteIngestTest extends CompaniesRemoteBaseTest {

	@Test
	public void testIngest() throws Exception {
		Assert.assertEquals(
				0,
				execute(PATH_HADOOP_BIN,
						new String[] { "jar", PATH_ASSEMBLY_INGEST_JAR, IngestDriver.class.getCanonicalName(),
								PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
	}

}