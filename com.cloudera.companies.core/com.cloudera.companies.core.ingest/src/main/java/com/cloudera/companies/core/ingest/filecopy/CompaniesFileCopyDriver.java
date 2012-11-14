package com.cloudera.companies.core.ingest.filecopy;

import java.io.File;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.common.hdfs.HDFSClientUtil;

public class CompaniesFileCopyDriver extends CompaniesDriver {

	private static Logger log = LoggerFactory.getLogger(CompaniesFileCopyDriver.class);

	public static final int RETURN_SUCCESS = 0;
	public static final int RETURN_FAILURE_MISSING_ARGS = 1;
	public static final int RETURN_FAILURE_INVALID_ARGS = 2;

	@Override
	public int run(String[] args) throws Exception {

		if (args == null || args.length != 2) {
			System.err.println("Usage: " + CompaniesFileCopyDriver.class.getSimpleName()
					+ " [generic options] <local-dir> <hdfs-dir>");
			ToolRunner.printGenericCommandUsage(System.err);
			return RETURN_FAILURE_MISSING_ARGS;
		}

		File localDir = new File(args[0]);
		if (!localDir.exists() || !localDir.isDirectory() || !localDir.canRead()) {
			System.err.println("Error: Local directory '" + args[0] + "' is not available");
			return RETURN_FAILURE_INVALID_ARGS;
		}

		Path hdfsDir = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(getConf());
		if (hdfs.exists(hdfsDir)) {
			if (!hdfs.isDirectory(hdfsDir)
					|| HDFSClientUtil.canPerformAction(hdfs, UserGroupInformation.getCurrentUser().getUserName(),
							UserGroupInformation.getCurrentUser().getGroupNames(), hdfsDir, FsAction.EXECUTE)) {
				System.err.println("Error: HDFS directory '" + args[1] + "' is not available");
				return RETURN_FAILURE_INVALID_ARGS;
			}
		} else {
			hdfs.mkdirs(hdfsDir, new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
		}

		return RETURN_SUCCESS;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CompaniesFileCopyDriver(), args));
	}
}
