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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.common.hdfs.HDFSClientUtil;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileKey;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileKeyCompositeComparator;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileKeyGroupComparator;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileKeyGroupPartitioner;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileZipFileInputFormat;
import com.cloudera.companies.core.ingest.IngestConstants.Counter;
import com.cloudera.companies.core.ingest.zip.IngestZipDriver;

public class IngestSeqDriver extends CompaniesDriver {

	private static Logger log = LoggerFactory.getLogger(IngestSeqDriver.class);

	public static final String NAMED_OUTPUT_PARTION_SEQ_FILES = "PartionedSequenceFiles";

	private static AtomicBoolean jobSubmitted = new AtomicBoolean(false);

	private Path hdfsOutputPath;
	private Set<Path> hdfsInputDirs;

	private String hdfsInputDir;
	private String hdfsOutputDir;

	private FileSystem hdfsFileSystem;
	
	public IngestSeqDriver() {
		super();
	}

	public IngestSeqDriver(Configuration conf) {
		super(conf);
	}

	@Override
	public int prepare(String[] args) {

		jobSubmitted.set(false);

		if (args == null || args.length != 2) {
			if (log.isErrorEnabled()) {
				log.error("Usage: " + IngestZipDriver.class.getSimpleName()
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

		 hdfsFileSystem = FileSystem.get(getConf());

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
			log.info("HDFS output directory [" + hdfsInputDir + "] validated as [" + hdfsInputPath + "]");
		}

		hdfsOutputPath = new Path(hdfsOutputDir);
		if (hdfsFileSystem.exists(hdfsOutputPath)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS output directory [" + hdfsOutputDir + "] already exists");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (!HDFSClientUtil.canDoAction(hdfsFileSystem, UserGroupInformation.getCurrentUser().getUserName(),
				UserGroupInformation.getCurrentUser().getGroupNames(), hdfsOutputPath.getParent(), FsAction.ALL)) {
			if (log.isErrorEnabled()) {
				log.error("HDFS parent of output directory [" + hdfsOutputDir
						+ "] has too restrictive permissions to write as user ["
						+ UserGroupInformation.getCurrentUser().getUserName() + "]");
			}
			return CompaniesDriver.RETURN_FAILURE_INVALID_ARGS;
		}
		if (log.isInfoEnabled()) {
			log.info("HDFS output directory [" + hdfsOutputDir + "] validated as [" + hdfsOutputPath + "]");
		}

		hdfsInputDirs = new HashSet<Path>();
		RemoteIterator<LocatedFileStatus> inputFiles = hdfsFileSystem.listFiles(hdfsInputPath, true);
		while (inputFiles.hasNext()) {
			LocatedFileStatus fileStatus = inputFiles.next();
			if (fileStatus.getPath().getName().equals(CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)) {
				String fileSuccessParent = fileStatus.getPath().getParent().toString();
				String fileSuccessParentSuffix = fileSuccessParent.substring(fileSuccessParent.indexOf(hdfsInputPath
						.toString()) + hdfsInputPath.toString().length());
				if (!hdfsFileSystem.exists(new Path(hdfsOutputPath.toString() + fileSuccessParentSuffix,
						CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME))) {
					hdfsInputDirs.add(fileStatus.getPath().getParent());
				}
			}
		}
		if (hdfsInputDirs.isEmpty()) {
			if (log.isInfoEnabled()) {
				log.info("No suitable files found to ingest");
			}
			return RETURN_FAILURE_INVALID_ARGS;
		}

		incramentCounter(IngestSeqDriver.class.getCanonicalName(), Counter.FILES_VALID, hdfsInputDirs.size());

		return RETURN_SUCCESS;
	}

	@Override
	public int execute() throws IOException, InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(getConf());

		job.setJobName(getClass().getSimpleName());

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
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, hdfsInputDirs.toArray(new Path[hdfsInputDirs.size()]));
		FileOutputFormat.setOutputPath(job, hdfsOutputPath);

		MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT_PARTION_SEQ_FILES, SequenceFileOutputFormat.class, Text.class,
				Text.class);

		job.setJarByClass(IngestSeqDriver.class);

		if (log.isInfoEnabled()) {
			log.info("Sequence file ingest job about to be submitted");
		}

		jobSubmitted.set(true);

		int exitCode = job.waitForCompletion(true) ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;

		importCounters(IngestSeqDriver.class.getCanonicalName(), job, new Counter[] { Counter.RECORDS_PROCESSED_VALID,
				Counter.RECORDS_PROCESSED_MALFORMED, Counter.RECORDS_PROCESSED_MALFORMED_KEY,
				Counter.RECORDS_PROCESSED_MALFORMED_DUPLICATE });

		if (log.isInfoEnabled()) {
			log.info("Sequence file ingest " + (exitCode == RETURN_SUCCESS ? "completed" : "failed"));
		}

		return exitCode;

	}

	@Override
	public int cleanup() throws IOException {

		hdfsFileSystem.close();

		return RETURN_SUCCESS;
	}

	@Override
	public int shutdown() {

		if (!jobSubmitted.get()) {
			if (log.isErrorEnabled()) {
				log.error("Halting before job submitted");
			}
		}

		return RETURN_SUCCESS;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new IngestSeqDriver(), args));
	}
}
