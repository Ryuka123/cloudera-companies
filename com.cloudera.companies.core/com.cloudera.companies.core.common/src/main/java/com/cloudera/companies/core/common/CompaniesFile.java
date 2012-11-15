package com.cloudera.companies.core.common;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVParser;

public class CompaniesFile {

	private static final Pattern FILE_NAME_PATTERN = Pattern
			.compile("BasicCompanyData-(20[0-9][0-9]-[0-1][0-9]-[0-3][0-9])-part([1-9])_([1-9]).zip");

	private static final String FILE_NAME_DATE_FORMAT = "yyyy-MM-dd";

	private static final CSVParser FILE_RECORD_PARSER = new CSVParser(',', '"', '\\', true, true);

	private File file;
	private int part;
	private int partTotal;
	private Date snapshotDate;

	public static CompaniesFile parseFileName(File file) throws IOException {
		if (file == null || !file.exists() || !file.isFile() || !file.canRead()) {
			throw new IllegalArgumentException("File [" + file + "] was not accessable");
		}
		Matcher fileNameMatcher = FILE_NAME_PATTERN.matcher(file.getName());
		if (fileNameMatcher.matches() && fileNameMatcher.groupCount() == 3) {
			try {
				return new CompaniesFile(file, Integer.parseInt(fileNameMatcher.group(2)),
						Integer.parseInt(fileNameMatcher.group(3)),
						new SimpleDateFormat(FILE_NAME_DATE_FORMAT).parse(fileNameMatcher.group(1)));
			} catch (Exception e) {
				throw new IOException("File name [" + file.getName()
						+ "] could not be parsed, nested root cause follows", e);
			}
		} else {
			throw new IOException("File name [" + file.getName() + "] did not match regex specification ["
					+ FILE_NAME_PATTERN + "]");
		}
	}

	public static String[] parseRecord(String record) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("Record passed was null");
		}
		return FILE_RECORD_PARSER.parseLine(record);
	}

	@Override
	public String toString() {
		try {
			StringBuilder string = new StringBuilder();
			string.append("[name=");
			string.append(file.getName());
			string.append(", snapshotDate=");
			string.append(new SimpleDateFormat(FILE_NAME_DATE_FORMAT).format(snapshotDate));
			string.append(", part=");
			string.append(part);
			string.append(", partTotal=");
			string.append(partTotal);
			string.append(", path=");
			string.append(file.getCanonicalPath());
			string.append("]");
			return string.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public CompaniesFile(File file, int part, int partTotal, Date snapshotDate) {
		super();
		this.file = file;
		this.part = part;
		this.partTotal = partTotal;
		this.snapshotDate = snapshotDate;
	}

	public File getFile() {
		return file;
	}

	public int getPart() {
		return part;
	}

	public int getPartTotal() {
		return partTotal;
	}

	public Date getSnapshotDate() {
		return snapshotDate;
	}

}
