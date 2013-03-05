package com.cloudera.companies.core.test;

import java.io.IOException;

import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.junit.After;
import org.junit.Before;

public abstract class CompaniesEmbeddedHiveTestCase extends CompaniesEmbeddedCoreTestCase {

  protected HiveInterface hive;

  public CompaniesEmbeddedHiveTestCase() throws IOException {
    super();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    hive = new HiveServer.HiveServerHandler();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    hive.shutdown();
    super.tearDown();
  }

}
