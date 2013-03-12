package com.cloudera.companies.core.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

    Assert.assertArrayEquals(new List[] { Arrays.asList(new String[] { "18" }), Arrays.asList(new String[] { "9" }) },
        executeAndFetchAll("/com/cloudera/companies/core/query/dml", "count.sql").toArray());

    Assert.assertArrayEquals(new List[] { Arrays.asList(new String[] { "1" }), Arrays.asList(new String[] { "1" }) },
        executeAndFetchAll("/com/cloudera/companies/core/query/dml", "duplicate.sql").toArray());

    Assert.assertArrayEquals(new List[] { Arrays.asList(new String[] { "4" }), Arrays.asList(new String[] { "3" }) },
        executeAndFetchAll("/com/cloudera/companies/core/query/dml", "malformed.sql").toArray());

    Assert
        .assertArrayEquals(
            new List[] {
                Arrays.asList(new String[] { "2" }),
                Arrays.asList(new String[] { "1" }),
                Arrays
                    .asList(new String[] { "\"01 PROPERTY INVESTMENT LTD\"	\"06291865\"	\"\"	\"\"	\"64 ST. JAMES'S STREET\"	\"\"	\"BRIGHTON\"	\"EAST SUSSEX\"	\"ENGLAND\"	\"BN2 1PJ\"	\"Private Limited Company\"	\"Active\"	\"United Kingdom\"	\"\"	\"25/06/2007\"	\"30\"	\"6\"	\"31/03/2013\"	\"30/06/2011\"	\"TOTAL EXEMPTION SMALL\"	\"23/07/2012\"	\"25/06/2011\"	\"5\"	\"5\"	\"0\"	\"0\"	\"7020 - Letting of own property\"	\"\"	\"\"	\"\"	\"0\"	\"0\"	\"http://business.data.gov.uk/id/company/06291865\"	\"26/07/2011\"	\"01 PROPERTY LTD\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	2012	05" }) },
            executeAndFetchAll("/com/cloudera/companies/core/query/dml", "select.sql").toArray());
  }
}
