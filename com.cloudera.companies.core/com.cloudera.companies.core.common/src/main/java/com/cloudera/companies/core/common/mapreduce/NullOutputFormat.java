package com.cloudera.companies.core.common.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NullOutputFormat<K, V> extends LazyOutputFormat<K, V> {

	public static void setOutputFormatClass(Job job) {
		job.setOutputFormatClass(LazyOutputFormat.class);
		job.getConfiguration().setClass(OUTPUT_FORMAT, NullOutputFormatInner.class, OutputFormat.class);
	}

	public static class NullOutputFormatInner<K, V> extends TextOutputFormat<K, V> {

		/**
		 * Overide so we tolerate the existance of the output dir, so that
		 * MultileOutputs writers can write to
		 */
		@Override
		public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
			Path outDir = getOutputPath(job);
			if (outDir == null) {
				throw new InvalidJobConfException("Output directory not set.");
			}
		}

	}
}
