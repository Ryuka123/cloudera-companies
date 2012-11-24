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

	protected static String HDFS_DIR = "target/test-hdfs";
	protected static String HDFS_DIR_TMP = HDFS_DIR + "/tmp";

	protected static Path HDFS_PATH = new Path(HDFS_DIR);
	protected static Path HDFS_PATH_TMP = new Path(HDFS_DIR_TMP);

	public CompaniesEmbeddedTestCase() throws IOException {
		super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);

		// Necessary to avoid warnings printed to console on OS-X
		System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
		System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");
	}

	public String getPathLocal(String pathRelativeToModuleRoot) {
		String pathRelativeToModuleRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToModuleRoot);
		return pathRelativeToModuleRootLessLeadingSlashes.equals("") ? (WORKING_DIR.length() < 2 ? "/" : WORKING_DIR
				.substring(0, WORKING_DIR.length() - 2)) : new Path(WORKING_DIR,
				pathRelativeToModuleRootLessLeadingSlashes).toUri().toString();
	}

	public String getPathHDFS(String pathRelativeToHDFSRoot) {
		String pathRelativeToHDFSRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToHDFSRoot);
		return pathRelativeToHDFSRootLessLeadingSlashes.equals("") ? HDFS_DIR : new Path(HDFS_PATH,
				pathRelativeToHDFSRootLessLeadingSlashes).toUri().toString();
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		FileSystem fileSystem = getFileSystem();
		if (fileSystem != null) {
			fileSystem.delete(HDFS_PATH, true);
			fileSystem.mkdirs(HDFS_PATH);
			fileSystem.mkdirs(HDFS_PATH_TMP);
			fileSystem.setPermission(HDFS_PATH_TMP, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
		}
	}

	@After
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
	}

	private String stripLeadingSlashes(String string) {
		int indexAfterLeadingSlash = 0;
		while (indexAfterLeadingSlash < string.length() && string.charAt(indexAfterLeadingSlash) == '/')
			++indexAfterLeadingSlash;
		return indexAfterLeadingSlash == 0 ? string : string.substring(indexAfterLeadingSlash, string.length());
	}

}
