package com.cloudera.companies.core.ingest;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.test.CompaniesEmbeddedTestCase;

public class IngestDriverTest extends CompaniesEmbeddedTestCase {

	private IngestDriver ingestDriver;
	private IngestFSCKDriver ingestFSCKDriver;

	public IngestDriverTest() throws IOException {
		super();
	}

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		ingestDriver = new IngestDriver(getFileSystem().getConf());
		ingestFSCKDriver = new IngestFSCKDriver(getFileSystem().getConf());
	}

	@Test
	public void test() throws Exception {
		Assert.assertEquals(
				CompaniesDriver.RETURN_SUCCESS,
				ingestDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP,
						PATH_HDFS_OUTPUT_DIR_SEQ }));
		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
		Assert.assertEquals(true, ingestFSCKDriver.testIntegretity(2, 8).isEmpty());
	}

}
