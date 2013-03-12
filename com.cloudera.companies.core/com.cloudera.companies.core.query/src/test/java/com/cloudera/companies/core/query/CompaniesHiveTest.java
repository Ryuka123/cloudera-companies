package com.cloudera.companies.core.query;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.ingest.IngestDriver;
import com.cloudera.companies.core.ingest.IngestFSCKDriver;
import com.cloudera.companies.core.ingest.IngestUtil;
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

    getConf().set("company.data.table", "company");
    getConf().set("company.data.location",
        PATH_LOCAL_WORKING_DIR + "/" + PATH_HDFS_OUTPUT_DIR_SEQ + "/" + IngestUtil.Counter.RECORDS_VALID.getPath());
    execute("/com/cloudera/companies/core/query/ddl", "create.sql");

    getConf().set("company.data.table", "company_duplicate");
    getConf().set("company.data.location",
        PATH_LOCAL_WORKING_DIR + "/" + PATH_HDFS_OUTPUT_DIR_SEQ + "/" + IngestUtil.Counter.RECORDS_DUPLICATE.getPath());
    execute("/com/cloudera/companies/core/query/ddl", "create.sql");

    getConf().set("company.data.table", "company_malformed");
    getConf().set("company.data.location",
        PATH_LOCAL_WORKING_DIR + "/" + PATH_HDFS_OUTPUT_DIR_SEQ + "/" + IngestUtil.Counter.RECORDS_MALFORMED.getPath());
    execute("/com/cloudera/companies/core/query/ddl", "create.sql");

    Assert.assertEquals(6, executeAndFetchAll("SHOW TABLES").size());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    getConf().set("company.data.table", "company");
    execute("/com/cloudera/companies/core/query/ddl", "drop.sql");
    getConf().set("company.data.table", "company_duplicate");
    execute("/com/cloudera/companies/core/query/ddl", "drop.sql");
    getConf().set("company.data.table", "company_malformed");
    execute("/com/cloudera/companies/core/query/ddl", "drop.sql");
    super.tearDown();
  }

  public void testQuery() throws Exception {
    Assert.assertEquals("18",
        executeAndFetchOne("/com/cloudera/companies/core/query/dml", "count_all_companies_all_snapshots.sql"));
    executeAndFetchOne("/com/cloudera/companies/core/query/dml", "count_all_companies_specific_snapshot.sql");
    executeAndFetchAll("/com/cloudera/companies/core/query/dml", "select_all_companies_all_snapshots.sql");
    executeAndFetchAll("/com/cloudera/companies/core/query/dml", "rank_registered_location.sql");
  }
}
