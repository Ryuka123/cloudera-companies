package com.cloudera.companies.core.test.local;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.ingest.IngestDriver;

public class CompaniesLocalIngestTest extends CompaniesLocalBaseTest {

	private IngestDriver ingestDriver;

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		ingestDriver = new IngestDriver(getFileSystem().getConf());
	}

	@Test
	public void testIngest() throws Exception {
		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				ingestDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
	}
}