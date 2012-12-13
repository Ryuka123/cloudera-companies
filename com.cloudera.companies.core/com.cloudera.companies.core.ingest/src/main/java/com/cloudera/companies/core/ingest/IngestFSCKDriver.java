package com.cloudera.companies.core.ingest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.common.CompaniesFileMetaData;
import com.cloudera.companies.core.common.hdfs.HDFSClientUtil;
import com.cloudera.companies.core.ingest.IngestUtil.Counter;
import com.cloudera.companies.core.ingest.seq.IngestSeqDriver;
import com.cloudera.companies.core.ingest.zip.IngestZipDriver;

public class IngestFSCKDriver extends CompaniesDriver {

	private static Logger log = LoggerFactory.getLogger(IngestFSCKDriver.class);

	protected static String CONF_FSCK_CLEAN = "companies.ingest.fsck.clean";

	private static final String COUNTER_GROUP_ZIP = IngestZipDriver.class.getPackage().getName() + "."
			+ IngestFSCKDriver.class.getSimpleName();
	private static final String COUNTER_GROUP_SEQ = IngestSeqDriver.class.getPackage().getName() + "."
			+ IngestFSCKDriver.class.getSimpleName();

	private String hdfsDirZip;
	private String hdfsDirSeq;

	private Map<String, Set<String>> fileZips;
	private Map<String, Set<String>> fileZipsPartials;
	private Map<String, Set<String>> fileZipsUnknowns;

	private Map<String, Set<String>> fileSeqs;
	private Map<String, Set<String>> fileSeqsPartials;
	private Map<String, Set<String>> fileSeqsUnknowns;
	private Map<String, Set<String>> fileSeqsErrors;

	private static AtomicBoolean isComplete = new AtomicBoolean(false);

	public IngestFSCKDriver() {
		super();
	}

	public IngestFSCKDriver(Configuration conf) {
		super(conf);
	}

	@Override
	public int prepare(String[] args) throws Exception {

		isComplete.set(false);

		if (args == null || args.length != 2) {
			if (log.isErrorEnabled()) {
				log.error("Usage: " + IngestFSCKDriver.class.getSimpleName()
						+ " [generic options] <hdfs-dir-zip> <hdfs-dir-seq>");
				ByteArrayOutputStream byteArrayPrintStream = new ByteArrayOutputStream();
				PrintStream printStream = new PrintStream(byteArrayPrintStream);
				ToolRunner.printGenericCommandUsage(printStream);
				log.error(byteArrayPrintStream.toString());
				printStream.close();
			}
			return CompaniesDriver.RETURN_FAILURE_MISSING_ARGS;
		}

		hdfsDirZip = args[0];
		hdfsDirSeq = args[1];

		return RETURN_SUCCESS;
	}

	@Override
	public int validate() throws IOException {

		FileSystem hdfsFileSystem = FileSystem.get(getConf());

		Path hdfsPathZip = new Path(hdfsDirZip);
		if (!hdfsFileSystem.exists(hdfsPathZip)) {
			hdfsFileSystem.mkdirs(hdfsPathZip);
		}
		if (!hdfsFileSystem.isDirectory(hdfsPathZip)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS zip directory [" + hdfsDirZip + "] is of incorrect type");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (!HDFSClientUtil.canDoAction(hdfsFileSystem, UserGroupInformation.getCurrentUser().getUserName(),
				UserGroupInformation.getCurrentUser().getGroupNames(), hdfsPathZip, FsAction.READ_EXECUTE)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS zip directory [" + hdfsDirZip + "] has too restrictive permissions to read as user ["
						+ UserGroupInformation.getCurrentUser().getUserName() + "]");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (log.isInfoEnabled()) {
			log.info("HDFS zip directory [" + hdfsDirZip + "] validated as [" + hdfsPathZip + "]");
		}

		Path hdfsPathSeq = new Path(hdfsDirSeq);
		if (!hdfsFileSystem.exists(hdfsPathSeq)) {
			hdfsFileSystem.mkdirs(hdfsPathSeq);
		}
		if (!hdfsFileSystem.isDirectory(hdfsPathSeq)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS sequence-file directory [" + hdfsPathSeq + "] is of incorrect type");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (!HDFSClientUtil.canDoAction(hdfsFileSystem, UserGroupInformation.getCurrentUser().getUserName(),
				UserGroupInformation.getCurrentUser().getGroupNames(), hdfsPathSeq, FsAction.READ_EXECUTE)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS sequence-file directory [" + hdfsPathSeq
						+ "] has too restrictive permissions to read as user ["
						+ UserGroupInformation.getCurrentUser().getUserName() + "]");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (log.isInfoEnabled()) {
			log.info("HDFS sequence-file directory [" + hdfsPathSeq + "] validated as [" + hdfsPathSeq + "]");
		}

		fileZips = new HashMap<String, Set<String>>();
		fileZipsPartials = new HashMap<String, Set<String>>();
		fileZipsUnknowns = new HashMap<String, Set<String>>();

		RemoteIterator<LocatedFileStatus> fileZipsItr = hdfsFileSystem.listFiles(hdfsPathZip, true);
		while (fileZipsItr.hasNext()) {
			LocatedFileStatus fileStatus = fileZipsItr.next();
			String file = fileStatus.getPath().getName();
			String parent = fileStatus.getPath().getParent().toString();
			String group = parent.substring(parent.indexOf(hdfsPathZip.toString()) + hdfsPathZip.toString().length());
			group = group.length() > 0 ? group.substring(1) : group;
			if (fileStatus.getPath().getName().equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)
					|| IngestUtil.isNamespacedFile(file, parent, group, Counter.FILES_SUCCESS)) {
				if (fileZips.get(parent) == null) {
					fileZips.put(parent, new HashSet<String>());
				}
				fileZips.get(parent).add(fileStatus.getPath().getName());
			} else {
				if (fileZipsUnknowns.get(parent) == null) {
					fileZipsUnknowns.put(parent, new HashSet<String>());
				}
				fileZipsUnknowns.get(parent).add(fileStatus.getPath().getName());
			}
		}
		Iterator<String> fileZipsGroupItr = fileZips.keySet().iterator();
		while (fileZipsGroupItr.hasNext()) {
			String parent = fileZipsGroupItr.next();
			if (!fileZips.get(parent).contains(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
				boolean foundSuccessMarker = false;
				Path groupParentPath = new Path(parent);
				while (!groupParentPath.isRoot()) {
					groupParentPath = groupParentPath.getParent();
					if (fileZips.get(groupParentPath.toString()) != null
							&& fileZips.get(groupParentPath.toString()).contains(
									CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
						foundSuccessMarker = true;
						break;
					}
				}
				if (!foundSuccessMarker) {
					if (fileZipsPartials.get(parent) == null) {
						fileZipsPartials.put(parent, fileZips.get(parent));
					} else {
						fileZipsPartials.get(parent).addAll(fileZips.get(parent));
					}
					fileZipsGroupItr.remove();
				}
			}
		}
		fileZipsGroupItr = fileZips.keySet().iterator();
		while (fileZipsGroupItr.hasNext()) {
			int filesNum = 0;
			int filesPartSum = 0;
			int filesPartTotal = Integer.MAX_VALUE;
			boolean filesPartTotalInconsitent = false;
			String parent = fileZipsGroupItr.next();
			for (String file : fileZips.get(parent)) {
				if (!file.equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
					filesNum++;
					CompaniesFileMetaData fileMetaData = CompaniesFileMetaData.parsePathZip(file, parent);
					filesPartSum += fileMetaData.getPart();
					if (filesPartTotal == Integer.MAX_VALUE) {
						filesPartTotal = fileMetaData.getPartTotal();
					} else {
						filesPartTotalInconsitent = filesPartTotal != fileMetaData.getPartTotal();
					}
				}
			}
			if (filesNum > 0
					&& (filesPartTotalInconsitent || filesPartSum != (filesPartTotal * (filesPartTotal + 1) / 2))) {
				if (fileZipsPartials.get(parent) == null) {
					fileZipsPartials.put(parent, fileZips.get(parent));
				} else {
					fileZipsPartials.get(parent).addAll(fileZips.get(parent));
				}
				fileZipsGroupItr.remove();
			}
		}

		fileSeqs = new HashMap<String, Set<String>>();
		fileSeqsPartials = new HashMap<String, Set<String>>();
		fileSeqsUnknowns = new HashMap<String, Set<String>>();
		fileSeqsErrors = new HashMap<String, Set<String>>();

		RemoteIterator<LocatedFileStatus> fileSeqsItr = hdfsFileSystem.listFiles(hdfsPathSeq, true);
		while (fileSeqsItr.hasNext()) {
			LocatedFileStatus fileStatus = fileSeqsItr.next();
			String file = fileStatus.getPath().getName();
			String parent = fileStatus.getPath().getParent().toString();
			String namespace = parent.substring(parent.indexOf(hdfsPathSeq.toString())
					+ hdfsPathSeq.toString().length());
			namespace = namespace.length() > 0 ? namespace.substring(1) : namespace;
			if (fileStatus.getPath().getName().equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)
					|| IngestUtil.isNamespacedFile(file, parent, namespace, Counter.RECORDS_VALID)) {
				if (fileSeqs.get(parent) == null) {
					fileSeqs.put(parent, new HashSet<String>());
				}
				fileSeqs.get(parent).add(fileStatus.getPath().getName());
			} else if (IngestUtil.isNamespacedFile(file, parent, namespace, Counter.RECORDS_MALFORMED)
					|| IngestUtil.isNamespacedFile(file, parent, namespace, Counter.RECORDS_DUPLICATE)) {
				if (fileSeqsErrors.get(parent) == null) {
					fileSeqsErrors.put(parent, new HashSet<String>());
				}
				fileSeqsErrors.get(parent).add(fileStatus.getPath().getName());
			} else {
				if (fileSeqsUnknowns.get(parent) == null) {
					fileSeqsUnknowns.put(parent, new HashSet<String>());
				}
				fileSeqsUnknowns.get(parent).add(fileStatus.getPath().getName());
			}
		}
		Iterator<String> fileSeqsGroupItr = fileSeqs.keySet().iterator();
		while (fileSeqsGroupItr.hasNext()) {
			String parent = fileSeqsGroupItr.next();
			if (!fileSeqs.get(parent).contains(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
				boolean foundSuccessMarker = false;
				Path groupParentPath = new Path(parent);
				while (!groupParentPath.isRoot()) {
					groupParentPath = groupParentPath.getParent();
					if (fileSeqs.get(groupParentPath.toString()) != null
							&& fileSeqs.get(groupParentPath.toString()).contains(
									CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
						foundSuccessMarker = true;
						break;
					}
				}
				if (!foundSuccessMarker) {
					if (fileSeqsPartials.get(parent) == null) {
						fileSeqsPartials.put(parent, fileSeqs.get(parent));
					} else {
						fileSeqsPartials.get(parent).addAll(fileSeqs.get(parent));
					}
					fileSeqsGroupItr.remove();
				}
			}
		}
		fileSeqsGroupItr = fileSeqs.keySet().iterator();
		while (fileSeqsGroupItr.hasNext()) {
			int filesNum = 0;
			int filesPartSum = 0;
			int filesPartTotal = Integer.MAX_VALUE;
			boolean filesPartTotalInconsitent = false;
			String parent = fileSeqsGroupItr.next();
			for (String file : fileSeqs.get(parent)) {
				if (!file.equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
					filesNum++;
					CompaniesFileMetaData fileMetaData = CompaniesFileMetaData.parsePathSeq(file, parent);
					filesPartSum += fileMetaData.getPart();
					if (filesPartTotal == Integer.MAX_VALUE) {
						filesPartTotal = fileMetaData.getPartTotal();
					} else {
						filesPartTotalInconsitent = filesPartTotal != fileMetaData.getPartTotal();
					}
				}
			}
			if (filesNum > 0
					&& (filesPartTotalInconsitent || filesPartSum != (filesPartTotal * (filesPartTotal + 1) / 2))) {
				if (fileSeqsPartials.get(parent) == null) {
					fileSeqsPartials.put(parent, fileSeqs.get(parent));
				} else {
					fileSeqsPartials.get(parent).addAll(fileSeqs.get(parent));
				}
				fileSeqsGroupItr.remove();
			}
		}

		return RETURN_SUCCESS;
	}

	@Override
	public int execute() throws Exception {

		incramentCounter(COUNTER_GROUP_ZIP, Counter.DATASETS, getCountGroup(fileZips));
		incramentCounter(COUNTER_GROUP_ZIP, Counter.FILES, getCount(fileZips) + getCount(fileZipsPartials)
				+ getCount(fileZipsUnknowns));
		incramentCounter(COUNTER_GROUP_ZIP, Counter.FILES_VALID, getCount(fileZips));
		incramentCounter(COUNTER_GROUP_ZIP, Counter.FILES_PARTIAL, getCount(fileZipsPartials));
		incramentCounter(COUNTER_GROUP_ZIP, Counter.FILES_UNKNOWN, getCount(fileZipsUnknowns));

		incramentCounter(COUNTER_GROUP_SEQ, Counter.DATASETS, getCountGroup(fileSeqs));
		incramentCounter(COUNTER_GROUP_SEQ, Counter.FILES, getCount(fileSeqs) + getCount(fileSeqsPartials)
				+ getCount(fileSeqsUnknowns));
		incramentCounter(COUNTER_GROUP_SEQ, Counter.FILES_VALID, getCount(fileSeqs));
		incramentCounter(COUNTER_GROUP_SEQ, Counter.FILES_ERROR, getCount(fileSeqsErrors));
		incramentCounter(COUNTER_GROUP_SEQ, Counter.FILES_PARTIAL, getCount(fileSeqsPartials));
		incramentCounter(COUNTER_GROUP_SEQ, Counter.FILES_UNKNOWN, getCount(fileSeqsUnknowns));

		int cleanZips = 0, cleanSeqs = 0;
		if (getConf().getBoolean(CONF_FSCK_CLEAN, false)) {
			cleanZips = executeClean(fileZipsPartials) + executeClean(fileZipsUnknowns);
			cleanSeqs = executeClean(fileSeqsPartials) + executeClean(fileSeqsUnknowns);
		}
		incramentCounter(COUNTER_GROUP_ZIP, Counter.FILES_CLEANED, cleanZips);
		incramentCounter(COUNTER_GROUP_SEQ, Counter.FILES_CLEANED, cleanSeqs);

		isComplete.set(true);

		return (getConf().getBoolean(CONF_FSCK_CLEAN, false) || (fileZipsPartials.size() + fileZipsUnknowns.size()
				+ fileSeqsPartials.size() + fileSeqsUnknowns.size()) == 0) ? RETURN_SUCCESS
				: RETURN_WARNING_DIRTY_INGEST;
	}

	@Override
	public int cleanup() throws IOException {
		return RETURN_SUCCESS;
	}

	@Override
	public int shutdown() {

		if (!isComplete.get()) {
			if (log.isErrorEnabled()) {
				log.error("Halting before completion, ingest may be only partially complete (and may contiue asyncrhonously)");
			}
		}

		return RETURN_SUCCESS;
	}

	public List<String> testIntegretity(int dataSetCount, int inputFilesCount, int outputInvalidFilesCount)
			throws IOException {
		List<String> errors = new ArrayList<String>();
		testIntegrity(errors, COUNTER_GROUP_ZIP, Counter.DATASETS, dataSetCount);
		testIntegrity(errors, COUNTER_GROUP_ZIP, Counter.FILES_VALID, inputFilesCount);
		testIntegrity(errors, COUNTER_GROUP_ZIP, Counter.FILES_PARTIAL, 0L);
		testIntegrity(errors, COUNTER_GROUP_ZIP, Counter.FILES_UNKNOWN, 0L);
		testIntegrity(errors, COUNTER_GROUP_SEQ, Counter.DATASETS, dataSetCount);
		testIntegrity(errors, COUNTER_GROUP_SEQ, Counter.FILES_VALID, dataSetCount);
		testIntegrity(errors, COUNTER_GROUP_SEQ, Counter.FILES_ERROR, outputInvalidFilesCount);
		testIntegrity(errors, COUNTER_GROUP_SEQ, Counter.FILES_PARTIAL, 0L);
		testIntegrity(errors, COUNTER_GROUP_SEQ, Counter.FILES_UNKNOWN, 0L);
		return errors;
	}

	private void testIntegrity(List<String> errors, String group, Enum<?> counter, long value) throws IOException {
		if (getCounter(group, counter) == null || getCounter(group, counter) != value) {
			errors.add(group + "." + counter.toString() + "==" + getCounter(group, counter) + "!=" + value);
		}
	}

	private int executeClean(Map<String, Set<String>> map) throws IOException {
		int count = 0;
		for (String parent : map.keySet()) {
			for (String file : map.get(parent)) {
				count += FileSystem.get(getConf()).delete(new Path(parent, file), true) ? 1 : 0;
				if (!FileSystem.get(getConf()).listFiles(new Path(parent), true).hasNext()) {
					FileSystem.get(getConf()).delete(new Path(parent), true);
				}
			}
		}
		return count;
	}

	private int getCount(Map<String, Set<String>> map) {
		int count = 0;
		for (String group : map.keySet()) {
			for (String name : map.get(group)) {
				count += (name.equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME) ? 0 : 1);
			}
		}
		return count;
	}

	private int getCountGroup(Map<String, Set<String>> map) {
		int count = 0;
		for (String group : map.keySet()) {
			for (String name : map.get(group)) {
				if (!name.equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
					count++;
					break;
				}
			}
		}
		return count;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new IngestFSCKDriver(), args));
	}
}
