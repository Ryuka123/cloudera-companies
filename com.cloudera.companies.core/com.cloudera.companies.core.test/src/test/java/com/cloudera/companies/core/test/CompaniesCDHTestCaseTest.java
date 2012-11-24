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
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR, getPathHDFS(""));
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR, getPathHDFS("/"));
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR, getPathHDFS("//"));
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR + "/tmp", getPathHDFS("tmp"));
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR + "/tmp", getPathHDFS("/tmp"));
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR + "/tmp", getPathHDFS("//tmp"));
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR + "/tmp", getPathHDFS("///tmp"));
		Assert.assertEquals(CompaniesEmbeddedTestCase.HDFS_DIR + "/tmp/tmp", getPathHDFS("///tmp//tmp"));
	}

	public void testPathLocal() {
		String localDir = new File(".").getAbsolutePath();
		localDir = localDir.substring(0, localDir.length() - 2);
		Assert.assertEquals(localDir, getPathLocal(""));
		Assert.assertEquals(localDir, getPathLocal("/"));
		Assert.assertEquals(localDir, getPathLocal("//"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("tmp"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("/tmp"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("//tmp"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("///tmp"));
		Assert.assertEquals(localDir + "/tmp/tmp", getPathLocal("///tmp//tmp"));
	}

	public void testFileSystem() throws IOException {
		String someDir = getPathHDFS("/some_dir");
		Assert.assertTrue(FileSystem.get(getFileSystem().getConf()).mkdirs(new Path(someDir)));
		Assert.assertTrue(new File(getPathLocal(someDir)).exists());
	}
}
