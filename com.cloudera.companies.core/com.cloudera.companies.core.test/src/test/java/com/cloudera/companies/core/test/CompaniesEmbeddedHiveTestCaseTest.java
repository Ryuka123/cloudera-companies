package com.cloudera.companies.core.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.Assert;

public class CompaniesEmbeddedHiveTestCaseTest extends CompaniesEmbeddedHiveTestCase {

  public CompaniesEmbeddedHiveTestCaseTest() throws IOException {
    super();
  }

  public void testHive() throws Exception {

    new File(PATH_LOCAL).mkdirs();
    File localDataFile = new File(PATH_LOCAL + "/somedata.csv");

    BufferedWriter writer = new BufferedWriter(new FileWriter(localDataFile));
    writer.write("1,1\n");
    writer.write("2,2\n");
    writer.write("3,3\n");
    writer.close();

    execute("/com/cloudera/companies/core/test/ddl", "create.sql");
    execute("LOAD DATA LOCAL INPATH '" + localDataFile.toString() + "' OVERWRITE INTO TABLE somedata");
    Assert.assertEquals("3", executeAndFetchOne("SELECT count(1) AS cnt FROM somedata"));
    Assert.assertEquals("2", executeAndFetchOne("SELECT col1 FROM somedata WHERE col2 = 2"));
    execute("DROP TABLE somedata");
  }

}
