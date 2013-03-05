package com.cloudera.companies.core.test;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CompaniesEmbeddedCoreTestCaseTest extends CompaniesEmbeddedCoreTestCase {

  public CompaniesEmbeddedCoreTestCaseTest() throws IOException {
    super();
  }

  public void testFileSystem() throws IOException {
    String someDir = CompaniesBaseTestCase.getPathHDFS("/some_dir");
    Assert.assertTrue(FileSystem.get(getFileSystem().getConf()).mkdirs(new Path(someDir)));
    Assert.assertTrue(new File(CompaniesBaseTestCase.getPathLocal(someDir)).exists());
  }

}
