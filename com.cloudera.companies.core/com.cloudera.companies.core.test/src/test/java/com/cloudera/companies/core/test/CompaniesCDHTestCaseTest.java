package com.cloudera.companies.core.test;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CompaniesCDHTestCaseTest extends CompaniesEmbeddedTestCase {

	public CompaniesCDHTestCaseTest() throws IOException {
		super();
	}

	public void testPathHDFS() {
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS, CompaniesBaseTestCase.getPathHDFS(""));
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS, CompaniesBaseTestCase.getPathHDFS("/"));
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS, CompaniesBaseTestCase.getPathHDFS("//"));
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("tmp"));
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("/tmp"));
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("//tmp"));
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS + "/tmp", CompaniesBaseTestCase.getPathHDFS("///tmp"));
		Assert.assertEquals(CompaniesBaseTestCase.PATH_HDFS + "/tmp/tmp",
				CompaniesBaseTestCase.getPathHDFS("///tmp//tmp"));
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

	public void testFileSystem() throws IOException {
		String someDir = CompaniesBaseTestCase.getPathHDFS("/some_dir");
		Assert.assertTrue(FileSystem.get(getFileSystem().getConf()).mkdirs(new Path(someDir)));
		Assert.assertTrue(new File(CompaniesBaseTestCase.getPathLocal(someDir)).exists());
	}
}
