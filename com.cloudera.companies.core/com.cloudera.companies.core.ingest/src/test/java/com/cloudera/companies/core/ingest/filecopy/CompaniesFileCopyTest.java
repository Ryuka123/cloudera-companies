package com.cloudera.companies.core.ingest.filecopy;

import java.io.IOException;

import junit.framework.Assert;

import com.cloudera.companies.core.test.CompaniesCDHTestCase;

public class CompaniesFileCopyTest extends CompaniesCDHTestCase {

	public CompaniesFileCopyTest() throws IOException {
		super();
	}

	public void testLaunchInvalid() throws Exception {
		Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
				new CompaniesFileCopyDriver().run(null));
		Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
				new CompaniesFileCopyDriver().run(new String[0]));
		Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
				new CompaniesFileCopyDriver().run(new String[] {}));
		Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
				new CompaniesFileCopyDriver().run(new String[] { getPathLocal("") }));
		Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
				new CompaniesFileCopyDriver().run(new String[] { getPathLocal("pom.xml") }));
		Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
				new CompaniesFileCopyDriver().run(new String[] { getPathLocal("pom.xml"), getPathHDFS("tmp") }));
		Assert.assertEquals(
				CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
				new CompaniesFileCopyDriver().run(new String[] { getPathLocal("/some_dir_that_does_not_exist"),
						getPathHDFS("tmp") }));
	}
}
