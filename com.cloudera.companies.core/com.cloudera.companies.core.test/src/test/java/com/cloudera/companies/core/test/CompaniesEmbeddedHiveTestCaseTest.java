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
    File localDataFile = new File(PATH_LOCAL + "somedata.csv");

    BufferedWriter writer = new BufferedWriter(new FileWriter(localDataFile));
    writer.write("1,1\n");
    writer.write("2,2\n");
    writer.write("3,3\n");
    writer.close();

    hive.execute("create table if not exists somedata  (col1 int, col2 int) row format delimited fields terminated by ',' stored as textfile");
    hive.execute("load data local inpath '" + localDataFile.toString() + "' overwrite into table somedata");
    hive.execute("select count(1) as cnt from somedata");
    Assert.assertEquals("3", hive.fetchOne());
    hive.execute("select col1 from somedata where col2 = 2");
    Assert.assertEquals("2", hive.fetchOne());
    hive.execute("drop table somedata");
  }

}
