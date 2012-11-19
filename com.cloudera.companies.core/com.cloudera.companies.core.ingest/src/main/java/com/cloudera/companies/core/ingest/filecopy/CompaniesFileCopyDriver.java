package com.cloudera.companies.core.ingest.filecopy;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ShutdownHookManager;
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

	private static AtomicBoolean isComplete = new AtomicBoolean(false);

	private Map<Long, FileSystem> fileSystems = new ConcurrentHashMap<Long, FileSystem>();

	@Override
	public int run(String[] args) throws Exception {

		long time = System.currentTimeMillis();

		isComplete.set(false);

		if (args == null || args.length != 2) {
			if (log.isErrorEnabled()) {
				log.error("Usage: " + CompaniesFileCopyDriver.class.getSimpleName()
						+ " [generic options] <local-input-dir> <hdfs-output-dir>");
				ByteArrayOutputStream byteArrayPrintStream = new ByteArrayOutputStream();
				PrintStream printStream = new PrintStream(byteArrayPrintStream);
				ToolRunner.printGenericCommandUsage(printStream);
				log.error(byteArrayPrintStream.toString());
				printStream.close();
			}
			return CompaniesDriver.RETURN_FAILURE_MISSING_ARGS;
		}

		String localInputDirPath = args[0];
		File localInputDir = new File(localInputDirPath);
		if (!localInputDir.exists()) {
			if (log.isErrorEnabled()) {
				log.error("Local input directory [" + localInputDirPath + "] does not exist");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (!localInputDir.isDirectory()) {
			if (log.isErrorEnabled()) {
				log.error("Local input directory [" + localInputDirPath + "] is of incorrect type");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (!localInputDir.canExecute()) {
			if (log.isErrorEnabled()) {
				log.error("Local input directory [" + localInputDirPath
						+ "] has too restrictive permissions to read as user ["
						+ UserGroupInformation.getCurrentUser().getUserName() + "]");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (log.isInfoEnabled()) {
			log.info("Local input directory [" + localInputDirPath + "] validated as ["
					+ localInputDir.getAbsolutePath() + "]");
		}

		String hdfsOutputDirPath = args[1];
		Path hdfsOutputDir = new Path(hdfsOutputDirPath);
		final FileSystem hdfs = getFileSystem();
		if (hdfs.exists(hdfsOutputDir)) {
			if (!hdfs.isDirectory(hdfsOutputDir)) {
				if (log.isErrorEnabled()) {
					log.error("HDFS output directory [" + hdfsOutputDirPath + "] is of incorrect type");
				}
				return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
			}
			if (!HDFSClientUtil.canPerformAction(hdfs, UserGroupInformation.getCurrentUser().getUserName(),
					UserGroupInformation.getCurrentUser().getGroupNames(), hdfsOutputDir, FsAction.ALL)) {
				if (log.isErrorEnabled()) {
					log.error("HDFS output directory [" + hdfsOutputDirPath
							+ "] has too restrictive permissions to read/write as user ["
							+ UserGroupInformation.getCurrentUser().getUserName() + "]");
				}
				return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
			}
		} else {
			hdfs.mkdirs(hdfsOutputDir, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
		}
		if (log.isInfoEnabled()) {
			log.info("HDFS output directory [" + hdfsOutputDirPath + "] validated as [" + hdfsOutputDir + "]");
		}

		Map<String, List<CompaniesFileMetaData>> companiesFileMetaDatasByGroup = new HashMap<String, List<CompaniesFileMetaData>>();
		for (File localInputFile : localInputDir.listFiles()) {
			if (localInputFile.isFile() && localInputFile.canRead()) {
				try {
					CompaniesFileMetaData companiesFileMetaData = CompaniesFileMetaData.parseFile(
							localInputFile.getName(), localInputFile.getParent());
					List<CompaniesFileMetaData> companiesFileMetaDatas;
					if ((companiesFileMetaDatas = companiesFileMetaDatasByGroup.get(companiesFileMetaData.getGroup())) == null) {
						companiesFileMetaDatasByGroup.put(companiesFileMetaData.getGroup(),
								(companiesFileMetaDatas = new ArrayList<CompaniesFileMetaData>()));
					}
					companiesFileMetaDatas.add(companiesFileMetaData);
				} catch (IOException e) {
					if (log.isWarnEnabled()) {
						log.warn("Failed to parse file [" + localInputFile.getCanonicalPath() + "]", e);
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
						companiesFileMetaData.getDirectory()),
						new Path(hdfsOutputDir, companiesFileMetaData.getGroup()));
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
					log.error("File copy mode [" + fileCopy.mode + "] timed out during ingest of local input file ["
							+ new Path(fileCopy.fromDirectory, fileCopy.fromFile) + "] to HDFS output file ["
							+ new Path(fileCopy.toDirectory, fileCopy.fromFile) + "]");
				}
				fileCopy.status = FileCopyStatus.FAILURE;
				fileCopySucces.remove(fileCopy);
				fileCopyFailure.add(fileCopy);
			}
			if (fileCopyFuture.isCancelled() || !fileCopy.status.equals(FileCopyStatus.SUCCESS)) {
				if (log.isErrorEnabled()) {
					log.error("Failures detected, rolling back ingest of local input file ["
							+ new Path(fileCopy.fromDirectory, fileCopy.fromFile) + "] to HDFS output file ["
							+ new Path(fileCopy.toDirectory, fileCopy.fromFile) + "]");
				}
				fileCopy.mode = FileCopyMode.CLEANUP;
				if (!fileCopy.call().status.equals(FileCopyStatus.SUCCESS)) {
					if (log.isErrorEnabled()) {
						log.error("Rollback failed for local input file ["
								+ new Path(fileCopy.fromDirectory, fileCopy.fromFile) + "] to HDFS output file ["
								+ new Path(fileCopy.toDirectory, fileCopy.fromFile) + "]");
					}
				}
				fileCopySucces.remove(fileCopy);
				fileCopyFailure.add(fileCopy);
			}
		}

		closeFileSystems();

		isComplete.set(true);

		if (log.isInfoEnabled()) {
			log.info("File ingest complete, successfully processing [" + fileCopySucces.size() + "] files, skipping ["
					+ fileCopySkips.size() + "] files and failing on [" + fileCopyFailure.size() + "] files with ["
					+ numberThreads + "] threads in [" + (System.currentTimeMillis() - time) + "] ms");
		}

		return fileCopyFailure.size() == 0 ? CompaniesDriver.RETURN_SUCCESS : CompaniesDriver.RETURN_FAILURE_RUNTIME;
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

	private void closeFileSystems() throws IOException {
		for (FileSystem fileSystem : fileSystems.values()) {
			fileSystem.close();
		}
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
				log.info("File copy mode [" + mode + "] returned [" + status + "] during ingest of local input file ["
						+ new Path(fromDirectory, fromFile) + "] to HDFS output file ["
						+ new Path(toDirectory, fromFile) + "]");
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

	public static void main(String[] args) throws Exception {
		ShutdownHookManager.get().addShutdownHook(new Runnable() {
			@Override
			public void run() {
				if (!isComplete.get()) {
					if (log.isErrorEnabled()) {
						log.error("Halting before completion, files may only be partly copied to HDFS");
					}
				}
			}
		}, RunJar.SHUTDOWN_HOOK_PRIORITY + 1);
		System.exit(ToolRunner.run(new CompaniesFileCopyDriver(), args));
	}
}
