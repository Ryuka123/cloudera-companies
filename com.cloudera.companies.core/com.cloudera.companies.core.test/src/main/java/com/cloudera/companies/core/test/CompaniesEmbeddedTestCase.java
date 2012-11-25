package com.cloudera.companies.core.test;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class CompaniesEmbeddedTestCase extends HadoopTestCase implements CompaniesBaseTest {

	public CompaniesEmbeddedTestCase() throws IOException {
		super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);

		// Necessary to avoid warnings printed to console on OS-X
		System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
		System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		FileSystem fileSystem = getFileSystem();
		if (fileSystem != null) {
			Path rootPath = new Path(CompaniesBaseTestCase.getPathHDFS("/"));
			Path tmpPath = new Path(CompaniesBaseTestCase.getPathHDFS("/tmp"));
			fileSystem.delete(rootPath, true);
			fileSystem.mkdirs(rootPath);
			fileSystem.mkdirs(tmpPath);
			fileSystem.setPermission(tmpPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
		}
	}

	@After
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
	}

}
