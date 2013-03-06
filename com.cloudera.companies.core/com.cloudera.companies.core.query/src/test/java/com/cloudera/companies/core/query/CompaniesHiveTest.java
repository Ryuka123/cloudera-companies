package com.cloudera.companies.core.query;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.ingest.IngestDriver;
import com.cloudera.companies.core.ingest.IngestFSCKDriver;
import com.cloudera.companies.core.test.CompaniesEmbeddedHiveTestCase;

public class CompaniesHiveTest extends CompaniesEmbeddedHiveTestCase {

  public CompaniesHiveTest() throws IOException {
    super();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    IngestDriver ingestDriver = new IngestDriver(getFileSystem().getConf());
    IngestFSCKDriver ingestFSCKDriver = new IngestFSCKDriver(getFileSystem().getConf());
    Assert
        .assertEquals(
            CompaniesDriver.RETURN_SUCCESS,
            ingestDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP,
                PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(true, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());

    execute("/com/cloudera/companies/core/query/ddl", "create.sql");
    execute("/com/cloudera/companies/core/query/ddl", "load.sql");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
//    execute("/com/cloudera/companies/core/query/ddl", "drop.sql");
  }

  public void testQuery() throws Exception {
    Assert.assertEquals("18", executeAndFetchOne("/com/cloudera/companies/core/query/dml", "count.sql"));
  }
}
