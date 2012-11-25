package com.cloudera.companies.core.test;

import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.After;
import org.junit.Before;

public class CompaniesLocalClusterTest extends ClusterMapReduceTestCase implements CompaniesBaseTest {

	private static final String WORKING_DIR_RUNTIME = PATH_LOCAL_WORKING_DIR + "/target/test-runtime/";

	public CompaniesLocalClusterTest() {
		super();
	}

	@Before
	@Override
	public void setUp() throws Exception {
		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", WORKING_DIR_RUNTIME + "build");
		}
		if (System.getProperty("hadoop.log.dir") == null) {
			System.setProperty("hadoop.log.dir", WORKING_DIR_RUNTIME + "log");
		}
		super.setUp();
	}

	@After
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
	}

}