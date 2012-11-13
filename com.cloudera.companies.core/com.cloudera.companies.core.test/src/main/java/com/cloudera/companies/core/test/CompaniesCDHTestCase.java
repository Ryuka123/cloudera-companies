package com.cloudera.companies.core.test;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.HadoopTestCase;

public abstract class CompaniesCDHTestCase extends HadoopTestCase {

	protected static Path HDFS_DIR = new Path("target/test-hdfs");
	protected static Path HDFS_DIR_TMP = new Path(HDFS_DIR, "tmp");

	public CompaniesCDHTestCase() throws IOException {
		super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);
	}

	public Path getPath(String path) {
		return new Path(HDFS_DIR, path);
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		FileSystem fileSystem = getFileSystem();
		if (fileSystem != null) {
			fileSystem.mkdirs(HDFS_DIR);
			fileSystem.mkdirs(HDFS_DIR_TMP);
			fileSystem.setPermission(HDFS_DIR_TMP, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
		}
	}

	@Override
	protected void tearDown() throws Exception {
		FileSystem fileSystem = getFileSystem();
		if (fileSystem != null) {
			fileSystem.delete(HDFS_DIR, true);
		}
		super.tearDown();
	}

}
