package com.cloudera.companies.core.test;

import java.io.File;

import junit.framework.Assert;

public class CompaniesBaseTestCaseTest {

  public void testPathHDFS() {
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS, CompaniesBaseTestCase.getPathHDFS(""));
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS, CompaniesBaseTestCase.getPathHDFS("/"));
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS, CompaniesBaseTestCase.getPathHDFS("//"));
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("tmp"));
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("/tmp"));
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("//tmp"));
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("///tmp"));
    Assert.assertEquals(CompaniesBaseTest.PATH_HDFS + "/tmp/tmp", CompaniesBaseTestCase.getPathHDFS("///tmp//tmp"));
  }

  public void testPathLocal() {
    String localDir = new File(".").getAbsolutePath();
    localDir = localDir.substring(0, localDir.length() - 2);
    Assert.assertEquals(localDir, CompaniesBaseTestCase.getPathLocal(""));
    Assert.assertEquals(localDir, CompaniesBaseTestCase.getPathLocal("/"));
    Assert.assertEquals(localDir, CompaniesBaseTestCase.getPathLocal("//"));
    Assert.assertEquals(localDir + "/tmp", CompaniesBaseTestCase.getPathLocal("tmp"));
    Assert.assertEquals(localDir + "/tmp", CompaniesBaseTestCase.getPathLocal("/tmp"));
    Assert.assertEquals(localDir + "/tmp", CompaniesBaseTestCase.getPathLocal("//tmp"));
    Assert.assertEquals(localDir + "/tmp", CompaniesBaseTestCase.getPathLocal("///tmp"));
    Assert.assertEquals(localDir + "/tmp/tmp", CompaniesBaseTestCase.getPathLocal("///tmp//tmp"));
  }

}
