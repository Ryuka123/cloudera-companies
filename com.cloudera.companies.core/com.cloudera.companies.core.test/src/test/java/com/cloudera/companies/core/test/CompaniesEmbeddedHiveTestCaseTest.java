package com.cloudera.companies.core.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CompaniesEmbeddedHiveTestCaseTest extends CompaniesEmbeddedHiveTestCase {

  public CompaniesEmbeddedHiveTestCaseTest() throws IOException {
    super();
  }

  public void testHive() throws Exception {

    new File(PATH_LOCAL).mkdirs();
    File localDataFile = new File(PATH_LOCAL + "somedata.csv");

    BufferedWriter writer = new BufferedWriter(new FileWriter(localDataFile));
    writer.write("1,1\n");
    writer.write("2,1\n");
    writer.close();

    hive.execute("create table  somedata  (col1 int, col2 int)");
    hive.execute("load data local inpath '" + localDataFile.toString() + "' into table somedata");
    hive.execute("select count(1) as cnt from somedata");
    String row = hive.fetchOne();
    assertEquals(row, "2");
    hive.execute("drop table somedata");
  }
}
