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
	}

	public String getPathLocal(String path) {
		return path == null || path.equals("") ? (LOCAL_DIR.length() < 2 ? "/" : LOCAL_DIR.substring(0,
				LOCAL_DIR.length() - 2)) : new Path(LOCAL_DIR, stripLeadingSlashes(path)).toUri().toString();
	}

	public String getPathHDFS(String path) {
		return path == null || path.equals("") ? HDFS_DIR : new Path(HDFS_PATH, stripLeadingSlashes(path)).toUri()
				.toString();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		FileSystem fileSystem = getFileSystem();
		if (fileSystem != null) {
			fileSystem.mkdirs(HDFS_PATH);
			fileSystem.mkdirs(HDFS_PATH_TMP);
			fileSystem.setPermission(HDFS_PATH_TMP, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
		}
	}

	@Override
	protected void tearDown() throws Exception {
		FileSystem fileSystem = getFileSystem();
		if (fileSystem != null) {
			fileSystem.delete(HDFS_PATH, true);
		}
		super.tearDown();
	}

	private String stripLeadingSlashes(String string) {
		int indexAfterLeadingSlash = 0;
		while (string.charAt(indexAfterLeadingSlash) == '/')
			++indexAfterLeadingSlash;
		return string.substring(indexAfterLeadingSlash, string.length());
	}

}
