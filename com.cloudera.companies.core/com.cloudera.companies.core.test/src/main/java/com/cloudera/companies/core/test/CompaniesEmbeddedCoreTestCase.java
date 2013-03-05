package com.cloudera.companies.core.test;

import java.io.IOException;

import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class CompaniesEmbeddedCoreTestCase extends HadoopTestCase implements CompaniesBaseTest {

  public CompaniesEmbeddedCoreTestCase() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);
    CompaniesBaseTestCase.init();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    CompaniesBaseTestCase.setUp(getFileSystem());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    CompaniesBaseTestCase.tearDown(getFileSystem());
    super.tearDown();
  }

}
