package com.cloudera.companies.core.ingest.seq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.common.hdfs.HDFSClientUtil;
import com.cloudera.companies.core.common.mapreduce.NullOutputFormat;
import com.cloudera.companies.core.ingest.IngestUtil;
import com.cloudera.companies.core.ingest.IngestUtil.Counter;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileKey;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileKeyCompositeComparator;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileKeyGroupComparator;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileKeyGroupPartitioner;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileZipFileInputFormat;

public class IngestSeqDriver extends CompaniesDriver {

	private static Logger log = LoggerFactory.getLogger(IngestSeqDriver.class);

	public static final String NAMED_OUTPUT_PARTION_SEQ_FILES = "PartionedSequenceFiles";

	private static AtomicBoolean isComplete = new AtomicBoolean(false);

	private Path hdfsOutputPath;
	private Set<Path> hdfsOutputDirs;
	private Set<Path> hdfsInputDirs;
	private Set<Path> hdfsSkippedDirs;

	private String hdfsInputDir;
	private String hdfsOutputDir;

	public IngestSeqDriver() {
		super();
	}

	public IngestSeqDriver(Configuration conf) {
		super(conf);
	}

	@Override
	public int prepare(String[] args) {

		isComplete.set(false);

		if (args == null || args.length != 2) {
			if (log.isErrorEnabled()) {
				log.error("Usage: " + IngestSeqDriver.class.getSimpleName()
						+ " [generic options] <hdfs-input-dir-zip> <hdfs-output-dir-seq>");
				ByteArrayOutputStream byteArrayPrintStream = new ByteArrayOutputStream();
				PrintStream printStream = new PrintStream(byteArrayPrintStream);
				ToolRunner.printGenericCommandUsage(printStream);
				log.error(byteArrayPrintStream.toString());
				printStream.close();
			}
			return CompaniesDriver.RETURN_FAILURE_MISSING_ARGS;
		}

		hdfsInputDir = args[0];
		hdfsOutputDir = args[1];

		return RETURN_SUCCESS;
	}

	@Override
	public int validate() throws IOException {

		FileSystem hdfsFileSystem = FileSystem.get(getConf());

		Path hdfsInputPath = new Path(hdfsInputDir);
		if (!hdfsFileSystem.exists(hdfsInputPath)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS input directory [" + hdfsInputDir + "] does not exist");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (!hdfsFileSystem.isDirectory(hdfsInputPath)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS input directory [" + hdfsInputDir + "] is of incorrect type");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (!HDFSClientUtil.canDoAction(hdfsFileSystem, UserGroupInformation.getCurrentUser().getUserName(),
				UserGroupInformation.getCurrentUser().getGroupNames(), hdfsInputPath, FsAction.READ_EXECUTE)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS input directory [" + hdfsInputDir
						+ "] has too restrictive permissions to read as user ["
						+ UserGroupInformation.getCurrentUser().getUserName() + "]");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (log.isInfoEnabled()) {
			log.info("HDFS input directory [" + hdfsInputDir + "] validated as [" + hdfsInputPath + "]");
		}

		hdfsOutputPath = new Path(hdfsOutputDir);
		if (!hdfsFileSystem.exists(hdfsOutputPath)) {
			hdfsFileSystem.mkdirs(hdfsOutputPath);
		}
		if (!HDFSClientUtil.canDoAction(hdfsFileSystem, UserGroupInformation.getCurrentUser().getUserName(),
				UserGroupInformation.getCurrentUser().getGroupNames(), hdfsOutputPath, FsAction.ALL)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS output directory [" + hdfsOutputDir
						+ "] has too restrictive permissions to write as user ["
						+ UserGroupInformation.getCurrentUser().getUserName() + "]");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (log.isInfoEnabled()) {
			log.info("HDFS output directory [" + hdfsOutputDir + "] validated as [" + hdfsOutputPath + "]");
		}

		hdfsInputDirs = new HashSet<Path>();
		hdfsSkippedDirs = new HashSet<Path>();
		RemoteIterator<LocatedFileStatus> inputFiles = hdfsFileSystem.listFiles(hdfsInputPath, true);
		while (inputFiles.hasNext()) {
			LocatedFileStatus fileStatus = inputFiles.next();
			if (fileStatus.getPath().getName().equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)
					&& fileStatus.getPath().getParent() != null && fileStatus.getPath().getParent().getParent() != null) {
				String fileSuccessParent = fileStatus.getPath().getParent().toString();
				String fileSuccessParentSuffix = fileSuccessParent.substring(fileSuccessParent.indexOf(hdfsInputPath
						.toString()) + hdfsInputPath.toString().length());
				if (!hdfsFileSystem.exists(new Path(hdfsOutputPath.toString() + fileSuccessParentSuffix,
						CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME))) {
					hdfsInputDirs.add(fileStatus.getPath().getParent());
				} else {
					hdfsSkippedDirs.add(fileStatus.getPath().getParent());
				}
			}
		}
		boolean ouputDirsExist = false;
		hdfsOutputDirs = new HashSet<Path>();
		for (Path inputPath : hdfsInputDirs) {
			Path ouputPath = new Path(hdfsOutputDir, IngestUtil.getNamespacedPath(
					Counter.RECORDS_VALID,
					inputPath.toString().substring(
							inputPath.toString().indexOf(hdfsInputPath.toString()) + hdfsInputPath.toString().length()
									+ 1)));
			if (hdfsFileSystem.exists(ouputPath)) {
				ouputDirsExist = true;
				if (log.isErrorEnabled()) {
					log.error("HDFS output directory [" + ouputPath + "] already exists");
				}
			} else {
				hdfsOutputDirs.add(ouputPath);
			}
		}
		if (ouputDirsExist) {
			if (log.isErrorEnabled()) {
				log.error("HDFS output directory [" + hdfsOutputDir
						+ "] contains sub output directoires that already exist");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}

		return RETURN_SUCCESS;
	}

	@Override
	public int execute() throws IOException, InterruptedException, ClassNotFoundException {

		boolean jobSuccess = false;
		int numberFailures = 0;
		Job job = null;

		isComplete.set(false);

		if (hdfsInputDirs.isEmpty()) {
			if (log.isWarnEnabled()) {
				log.warn("No suitable files found to ingest");
			}
		} else {

			job = Job.getInstance(getConf());

			job.setJobName(getClass().getSimpleName());

			job.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", Boolean.FALSE.toString());
			job.getConfiguration().set("hadoop.job.history.user.location", Boolean.FALSE.toString());

			job.setPartitionerClass(CompaniesFileKeyGroupPartitioner.class);
			job.setGroupingComparatorClass(CompaniesFileKeyGroupComparator.class);
			job.setSortComparatorClass(CompaniesFileKeyCompositeComparator.class);

			job.setMapOutputKeyClass(CompaniesFileKey.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(IngestSeqMapper.class);
			job.setReducerClass(IngestSeqReducer.class);

			job.setInputFormatClass(CompaniesFileZipFileInputFormat.class);
			NullOutputFormat.setOutputFormatClass(job);

			job.setNumReduceTasks(hdfsInputDirs.size());

			FileInputFormat.setInputPaths(job, hdfsInputDirs.toArray(new Path[hdfsInputDirs.size()]));
			FileOutputFormat.setOutputPath(job, hdfsOutputPath);

			MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT_PARTION_SEQ_FILES, SequenceFileOutputFormat.class,
					Text.class, Text.class);

			job.setJarByClass(IngestSeqDriver.class);

			if (log.isInfoEnabled()) {
				log.info("Sequence file ingest job about to be submitted");
			}

			jobSuccess = job.waitForCompletion(log.isInfoEnabled());

			for (Path ouputPath : hdfsOutputDirs) {
				if (FileSystem.get(getConf()).exists(ouputPath)) {
					if (jobSuccess) {
						FileSystem.get(getConf()).create(
								new Path(ouputPath, CompaniesDriver.CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME));
					}
				} else {
					numberFailures++;
					if (log.isErrorEnabled()) {
						log.error("Expected output directory [" + ouputPath + "] not found");
					}
				}
			}
		}

		isComplete.set(true);

		incramentCounter(IngestSeqDriver.class.getCanonicalName(), Counter.DATASETS, hdfsInputDirs.size()
				+ hdfsSkippedDirs.size());
		incramentCounter(IngestSeqDriver.class.getCanonicalName(), Counter.DATASETS_SUCCESS, hdfsInputDirs.size()
				- numberFailures);
		incramentCounter(IngestSeqDriver.class.getCanonicalName(), Counter.DATASETS_SKIP, hdfsSkippedDirs.size());
		incramentCounter(IngestSeqDriver.class.getCanonicalName(), Counter.DATASETS_FAILURE, numberFailures);

		if (job != null) {
			importCounters(IngestSeqDriver.class.getCanonicalName(), job, new Counter[] { Counter.RECORDS,
					Counter.RECORDS_VALID, Counter.RECORDS_MALFORMED, Counter.RECORDS_DUPLICATE });
		}

		if (log.isInfoEnabled()) {
			log.info("Sequence file ingest " + (jobSuccess ? "completed" : "failed"));
		}

		return jobSuccess ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;

	}

	@Override
	public int cleanup() throws IOException {
		return RETURN_SUCCESS;
	}

	@Override
	public int shutdown() {

		if (!isComplete.get()) {
			if (log.isErrorEnabled()) {
				log.error("Halting before completion, files may only be partly copied to HDFS");
			}
		}

		return RETURN_SUCCESS;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new IngestSeqDriver(), args));
	}
}
