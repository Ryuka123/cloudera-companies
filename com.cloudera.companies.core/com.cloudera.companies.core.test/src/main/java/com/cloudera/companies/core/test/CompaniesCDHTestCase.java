package com.cloudera.companies.core.test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.HadoopTestCase;

public abstract class CompaniesCDHTestCase extends HadoopTestCase {

	protected static String LOCAL_DIR = new File(".").getAbsolutePath();

	protected static String HDFS_DIR = "target/test-hdfs";
	protected static String HDFS_DIR_TMP = HDFS_DIR + "/tmp";

	protected static Path HDFS_PATH = new Path(HDFS_DIR);
	protected static Path HDFS_PATH_TMP = new Path(HDFS_DIR_TMP);

	public CompaniesCDHTestCase() throws IOException {
		super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);
		System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
		System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");
	}

	public String getPathLocal(String pathRelativeToModuleRoot) {
		String pathRelativeToModuleRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToModuleRoot);
		return pathRelativeToModuleRootLessLeadingSlashes.equals("") ? (LOCAL_DIR.length() < 2 ? "/" : LOCAL_DIR
				.substring(0, LOCAL_DIR.length() - 2))
				: new Path(LOCAL_DIR, pathRelativeToModuleRootLessLeadingSlashes).toUri().toString();
	}

	public String getPathHDFS(String pathRelativeToHDFSRoot) {
		String pathRelativeToHDFSRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToHDFSRoot);
		return pathRelativeToHDFSRootLessLeadingSlashes.equals("") ? HDFS_DIR : new Path(HDFS_PATH,
				pathRelativeToHDFSRootLessLeadingSlashes).toUri().toString();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		FileSystem fileSystem = getFileSystem();
		if (fileSystem != null) {
			fileSystem.delete(HDFS_PATH, true);
			fileSystem.mkdirs(HDFS_PATH);
			fileSystem.mkdirs(HDFS_PATH_TMP);
			fileSystem.setPermission(HDFS_PATH_TMP, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
		}
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	private String stripLeadingSlashes(String string) {
		int indexAfterLeadingSlash = 0;
		while (indexAfterLeadingSlash < string.length() && string.charAt(indexAfterLeadingSlash) == '/')
			++indexAfterLeadingSlash;
		return indexAfterLeadingSlash == 0 ? string : string.substring(indexAfterLeadingSlash, string.length());
	}

}
