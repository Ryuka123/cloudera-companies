package com.cloudera.companies.core.test;

import org.apache.hadoop.fs.Path;

public abstract class CompaniesBaseTestCase implements CompaniesBaseTest {

	protected static String PATH_HDFS = "target/test-hdfs";

	public static String getPathLocal(String pathRelativeToModuleRoot) {
		String pathRelativeToModuleRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToModuleRoot);
		return pathRelativeToModuleRootLessLeadingSlashes.equals("") ? (PATH_LOCAL_WORKING_DIR.length() < 2 ? "/"
				: PATH_LOCAL_WORKING_DIR.substring(0, PATH_LOCAL_WORKING_DIR.length() - 2)) : new Path(
				PATH_LOCAL_WORKING_DIR, pathRelativeToModuleRootLessLeadingSlashes).toUri().toString();
	}

	public static String getPathHDFS(String pathRelativeToHDFSRoot) {
		String pathRelativeToHDFSRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToHDFSRoot);
		return pathRelativeToHDFSRootLessLeadingSlashes.equals("") ? PATH_HDFS : new Path(PATH_HDFS,
				pathRelativeToHDFSRootLessLeadingSlashes).toUri().toString();
	}

	private static String stripLeadingSlashes(String string) {
		int indexAfterLeadingSlash = 0;
		while (indexAfterLeadingSlash < string.length() && string.charAt(indexAfterLeadingSlash) == '/')
			++indexAfterLeadingSlash;
		return indexAfterLeadingSlash == 0 ? string : string.substring(indexAfterLeadingSlash, string.length());
	}

}
