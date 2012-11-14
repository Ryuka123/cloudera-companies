package com.cloudera.companies.core.test;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

public class CompaniesCDHTestCaseTest extends CompaniesCDHTestCase {

	public CompaniesCDHTestCaseTest() throws IOException {
		super();
	}

	public void testPathHDFS() {
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR, getPathHDFS(null));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR, getPathHDFS(""));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR, getPathHDFS("/"));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR, getPathHDFS("//"));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR + "/tmp", getPathHDFS("tmp"));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR + "/tmp", getPathHDFS("/tmp"));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR + "/tmp", getPathHDFS("//tmp"));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR + "/tmp", getPathHDFS("///tmp"));
		Assert.assertEquals(CompaniesCDHTestCase.HDFS_DIR + "/tmp/tmp", getPathHDFS("///tmp//tmp"));
	}

	public void testPathLocal() {
		String localDir = new File(".").getAbsolutePath();
		localDir = localDir.substring(0, localDir.length() - 2);
		Assert.assertEquals(localDir, getPathLocal(null));
		Assert.assertEquals(localDir, getPathLocal(""));
		Assert.assertEquals(localDir, getPathLocal("/"));
		Assert.assertEquals(localDir, getPathLocal("//"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("tmp"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("/tmp"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("//tmp"));
		Assert.assertEquals(localDir + "/tmp", getPathLocal("///tmp"));
		Assert.assertEquals(localDir + "/tmp/tmp", getPathLocal("///tmp//tmp"));
	}

}
