package com.cloudera.companies.core.ingest.filecopy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.common.CompaniesFileMetaData;
import com.cloudera.companies.core.common.hdfs.HDFSClientUtil;

public class CompaniesFileCopyDriver extends CompaniesDriver {

	private static Logger log = LoggerFactory.getLogger(CompaniesFileCopyDriver.class);

	public static final String CONF_TIMEOUT_SECS = "companies.ingest.timeout.secs";
	public static final String CONF_THREAD_NUMBER = "companies.ingest.thread.number";

	public static final int RETURN_SUCCESS = 0;
	public static final int RETURN_FAILURE_MISSING_ARGS = 1;
	public static final int RETURN_FAILURE_INVALID_ARGS = 2;
	public static final int RETURN_FAILURE_RUNTIME = 3;

	private Map<Long, FileSystem> fileSystems = new ConcurrentHashMap<Long, FileSystem>();

	@Override
	public int run(String[] args) throws Exception {

		long time = System.currentTimeMillis();

		if (args == null || args.length != 2) {
			System.err.println("Usage: " + CompaniesFileCopyDriver.class.getSimpleName()
					+ " [generic options] <local-dir> <hdfs-dir>");
			ToolRunner.printGenericCommandUsage(System.err);
			return RETURN_FAILURE_MISSING_ARGS;
		}

		String localDirPath = args[0];
		File localDir = new File(localDirPath);
		if (!localDir.exists()) {
			System.err.println("Error: Local directory '" + localDirPath + "' does not exist");
			return RETURN_FAILURE_INVALID_ARGS;
		}
		if (!localDir.isDirectory()) {
			System.err.println("Error: Local directory '" + localDirPath + "' is of incorrect type");
			return RETURN_FAILURE_INVALID_ARGS;
		}
		if (!localDir.canExecute()) {
			System.err.println("Error: Local directory '" + localDirPath
					+ "' has too restrictive permissions to read as user '"
					+ UserGroupInformation.getCurrentUser().getUserName() + "'");
			return RETURN_FAILURE_INVALID_ARGS;
		}
		if (log.isInfoEnabled()) {
			log.info("Local directory [" + localDirPath + "] validated as [" + localDir.getAbsolutePath() + "]");
		}

		String hdfsDirPath = args[1];
		Path hdfsDir = new Path(hdfsDirPath);
		final FileSystem hdfs = getFileSystem();
		if (hdfs.exists(hdfsDir)) {
			if (!hdfs.isDirectory(hdfsDir)) {
				System.err.println("Error: HDFS directory '" + hdfsDirPath + "' is of incorrect type");
				return RETURN_FAILURE_INVALID_ARGS;
			}
			if (!HDFSClientUtil.canPerformAction(hdfs, UserGroupInformation.getCurrentUser().getUserName(),
					UserGroupInformation.getCurrentUser().getGroupNames(), hdfsDir, FsAction.ALL)) {
				System.err.println("Error: HDFS directory '" + hdfsDirPath
						+ "' has too restrictive permissions to read/write as user '"
						+ UserGroupInformation.getCurrentUser().getUserName() + "'");
				return RETURN_FAILURE_INVALID_ARGS;
			}
		} else {
			hdfs.mkdirs(hdfsDir, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
		}
		if (log.isInfoEnabled()) {
			log.info("HDFS directory [" + hdfsDirPath + "] validated as [" + hdfsDir + "]");
		}

		Map<String, List<CompaniesFileMetaData>> companiesFileMetaDatasByGroup = new HashMap<String, List<CompaniesFileMetaData>>();
		for (File localFile : localDir.listFiles()) {
			if (localFile.isFile() && localFile.canRead()) {
				try {
					CompaniesFileMetaData companiesFileMetaData = CompaniesFileMetaData.parseFile(localFile.getName(),
							localFile.getParent());
					List<CompaniesFileMetaData> companiesFileMetaDatas;
					if ((companiesFileMetaDatas = companiesFileMetaDatasByGroup.get(companiesFileMetaData.getGroup())) == null) {
						companiesFileMetaDatasByGroup.put(companiesFileMetaData.getGroup(),
								(companiesFileMetaDatas = new ArrayList<CompaniesFileMetaData>()));
					}
					companiesFileMetaDatas.add(companiesFileMetaData);
				} catch (IOException e) {
					if (log.isWarnEnabled()) {
						log.warn("Failed to parse file [" + localFile.getCanonicalPath() + "]", e);
					}
				}
			}
		}

		Set<FileCopy> fileCopySucces = new HashSet<CompaniesFileCopyDriver.FileCopy>();
		Set<FileCopy> fileCopySkips = new HashSet<CompaniesFileCopyDriver.FileCopy>();
		Set<FileCopy> fileCopyFailure = new HashSet<CompaniesFileCopyDriver.FileCopy>();
		for (String companiesFileGroup : companiesFileMetaDatasByGroup.keySet()) {
			for (CompaniesFileMetaData companiesFileMetaData : companiesFileMetaDatasByGroup.get(companiesFileGroup)) {
				FileCopy fileCopy = new FileCopy(new Path(companiesFileMetaData.getName()), new Path(
						companiesFileMetaData.getDirectory()), new Path(hdfsDir, companiesFileMetaData.getGroup()));
				switch (fileCopy.call().status) {
				case SUCCESS:
					fileCopySucces.add(fileCopy);
					break;
				case SKIP:
					fileCopySkips.add(fileCopy);
					break;
				case FAILURE:
					fileCopyFailure.add(fileCopy);
					break;
				}
			}
		}

		int numberThreads = getConf().getInt(CONF_THREAD_NUMBER, 1);
		List<Future<FileCopy>> copyFileFutures = new ArrayList<Future<FileCopy>>();
		ExecutorService copyFileExector = new ThreadPoolExecutor(numberThreads, numberThreads, 0L,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		for (FileCopy fileCopy : fileCopySucces) {
			fileCopy.mode = FileCopyMode.EXECUTE;
			copyFileFutures.add(copyFileExector.submit(fileCopy));
		}
		copyFileExector.shutdown();
		if (!copyFileExector.awaitTermination(getConf().getInt(CONF_TIMEOUT_SECS, 600) * fileCopySucces.size(),
				TimeUnit.SECONDS)) {
			copyFileExector.shutdownNow();
		}

		for (Future<FileCopy> fileCopyFuture : copyFileFutures) {
			FileCopy fileCopy = fileCopyFuture.get();
			if (fileCopyFuture.isCancelled()) {
				if (log.isErrorEnabled()) {
					log.error("File copy mode [" + fileCopy.mode + "] timed out during ingest of local file ["
							+ new Path(fileCopy.fromDirectory, fileCopy.fromFile) + "] to HDFS file ["
							+ new Path(fileCopy.toDirectory, fileCopy.fromFile) + "]");
				}
				fileCopy.status = FileCopyStatus.FAILURE;
				fileCopySucces.remove(fileCopy);
				fileCopyFailure.add(fileCopy);
			}
			if (fileCopyFuture.isCancelled() || !fileCopy.status.equals(FileCopyStatus.SUCCESS)) {
				if (log.isErrorEnabled()) {
					log.error("Failures detected, rolling back ingest of local file ["
							+ new Path(fileCopy.fromDirectory, fileCopy.fromFile) + "] to HDFS file ["
							+ new Path(fileCopy.toDirectory, fileCopy.fromFile) + "]");
				}
				fileCopy.mode = FileCopyMode.CLEANUP;
				if (!fileCopy.call().status.equals(FileCopyStatus.SUCCESS)) {
					if (log.isErrorEnabled()) {
						log.error("Rollback failed for local file ["
								+ new Path(fileCopy.fromDirectory, fileCopy.fromFile) + "] to HDFS file ["
								+ new Path(fileCopy.toDirectory, fileCopy.fromFile) + "]");
					}
				}
				fileCopySucces.remove(fileCopy);
				fileCopyFailure.add(fileCopy);
			}
		}

		if (log.isInfoEnabled()) {
			log.info("File ingest complete, successfully processing [" + fileCopySucces.size() + "] files, skipping ["
					+ fileCopySkips.size() + "] files and failing on [" + fileCopyFailure.size() + "] files with ["
					+ numberThreads + "] threads in [" + (System.currentTimeMillis() - time) + "] ms");
		}

		return fileCopyFailure.size() == 0 ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;
	}

	/**
	 * 
	 * Get a {@link FileSystem} from the local thread store cache
	 * 
	 * @return
	 * @throws IOException
	 */
	private FileSystem getFileSystem() throws IOException {
		FileSystem fileSystem = fileSystems.get(Thread.currentThread().getId());
		if (fileSystem == null) {
			fileSystems.put(Thread.currentThread().getId(), (fileSystem = FileSystem.newInstance(getConf())));
		}
		return fileSystem;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CompaniesFileCopyDriver(), args));
	}

	public enum FileCopyMode {
		PREPARE, EXECUTE, CLEANUP
	}

	public enum FileCopyStatus {
		SUCCESS, SKIP, FAILURE
	}

	private class FileCopy implements Callable<FileCopy> {

		private Path fromFile;
		private Path fromDirectory;
		private Path toDirectory;
		private FileCopyMode mode;
		private FileCopyStatus status;

		public FileCopy(Path fromFile, Path fromDirectory, Path toDirectory) throws IOException {
			this.fromFile = fromFile;
			this.fromDirectory = fromDirectory;
			this.toDirectory = toDirectory;
			this.mode = FileCopyMode.PREPARE;
		}

		@Override
		public FileCopy call() throws Exception {
			try {
				status = FileCopyStatus.FAILURE;
				switch (mode) {
				case PREPARE:
					if (getFileSystem().mkdirs(toDirectory)) {
						if (!getFileSystem().exists(new Path(toDirectory, fromFile))) {
							status = FileCopyStatus.SUCCESS;
						} else {
							status = FileCopyStatus.SKIP;
						}
					}
					break;
				case EXECUTE:
					getFileSystem().copyFromLocalFile(new Path(fromDirectory, fromFile), toDirectory);
					status = FileCopyStatus.SUCCESS;
					break;
				case CLEANUP:
					getFileSystem().delete(new Path(toDirectory, fromFile), false);
					status = FileCopyStatus.SUCCESS;
					break;
				}
			} catch (IOException e) {
				if (log.isErrorEnabled()) {
					log.error("File copy mode [" + mode + "] failed, exception to follow", e);
				}
			}
			if (log.isInfoEnabled()) {
				log.info("File copy mode [" + mode + "] returned [" + status + "] during ingest of local file ["
						+ new Path(fromDirectory, fromFile) + "] to HDFS file [" + new Path(toDirectory, fromFile)
						+ "]");
			}
			return this;
		}

		@Override
		public int hashCode() {
			return (fromFile.toString() + fromDirectory.toString() + toDirectory.toString()).hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof FileCopy) {
				FileCopy that = (FileCopy) obj;
				return fromFile.toString().equals(that.fromFile.toString())
						&& fromDirectory.toString().equals(that.fromDirectory.toString())
						&& toDirectory.toString().equals(that.toDirectory.toString());
			}
			return false;
		}
	}
}
