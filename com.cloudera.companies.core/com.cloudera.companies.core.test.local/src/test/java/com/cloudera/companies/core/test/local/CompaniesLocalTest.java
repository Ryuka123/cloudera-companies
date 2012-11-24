package com.cloudera.companies.core.test.local;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.ingest.zip.IngestZipDriver;
import com.cloudera.companies.core.test.CompaniesLocalClusterTest;

public class CompaniesLocalTest extends CompaniesLocalClusterTest {

	private IngestZipDriver ingestZipDriver;

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		ingestZipDriver = new IngestZipDriver();
		ingestZipDriver.setConf(getFileSystem().getConf());
	}

	@Test
	public void test() throws Exception {

		String inputDir = "/Users/graham/_/dev/personal/cloudera-companies/com.cloudera.companies.data/src/main/resources/data/basiccompany/sample/zip";
		String outputDir = "target";

		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS, ingestZipDriver.run(new String[] { inputDir, outputDir }));

	}
}