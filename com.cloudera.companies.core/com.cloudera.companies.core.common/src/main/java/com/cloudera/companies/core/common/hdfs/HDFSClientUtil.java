package com.cloudera.companies.core.common.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class HDFSClientUtil {

	public static boolean canPerformAction(FileSystem hdfs, String user, String[] groups, Path path, FsAction action)
			throws IOException {
		FileStatus status = hdfs.getFileStatus(path);
		FsPermission permission = status.getPermission();
		if (permission.getOtherAction().implies(action)) {
			return true;
		}
		for (String group : groups) {
			if (group.equals(status.getGroup()) && permission.getGroupAction().implies(action)) {
				return true;
			}
		}
		if (user.equals(status.getOwner()) && permission.getUserAction().implies(action)) {
			return true;
		}
		return false;
	}

}
