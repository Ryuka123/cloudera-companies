package com.cloudera.companies.core.common;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVParser;

public class CompaniesFileMetaData {

	public static final int FILE_FIELDS_NUMBER = 54;
	
	private static final Pattern FILE_NAME_PATTERN = Pattern
			.compile("BasicCompanyData-(20[0-9][0-9]-[0-1][0-9]-[0-3][0-9])-part([1-9])_([1-9]).zip");

	private static final String FILE_NAME_DATE_FORMAT = "yyyy-MM-dd";
	private static final String GROUP_FORMAT = "yyyy/MM";

	private static final CSVParser FILE_RECORD_PARSER = new CSVParser(',', '"', '\\', true, true);

	private String name;
	private String directory;
	private String group;
	private int part;
	private int partTotal;
	private Date snapshotDate;

	public static CompaniesFileMetaData parseFile(String name, String directory) throws IOException {
		if (name == null) {
			throw new IllegalArgumentException("null name");
		}
		if (directory == null) {
			throw new IllegalArgumentException("null directory");
		}
		Matcher fileNameMatcher = FILE_NAME_PATTERN.matcher(name);
		if (fileNameMatcher.matches() && fileNameMatcher.groupCount() == 3) {
			try {
				return new CompaniesFileMetaData(name, directory,
						new SimpleDateFormat(GROUP_FORMAT).format(new SimpleDateFormat(FILE_NAME_DATE_FORMAT)
								.parse(fileNameMatcher.group(1))), Integer.parseInt(fileNameMatcher.group(2)),
						Integer.parseInt(fileNameMatcher.group(3)),
						new SimpleDateFormat(FILE_NAME_DATE_FORMAT).parse(fileNameMatcher.group(1)));
			} catch (Exception e) {
				throw new IOException("File name [" + name + "] could not be parsed, nested root cause follows", e);
			}
		} else {
			throw new IOException("File name [" + name + "] did not match regex specification [" + FILE_NAME_PATTERN
					+ "]");
		}
	}

	public static String[] parseRecord(String record) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("null record");
		}
		return FILE_RECORD_PARSER.parseLine(record);
	}

	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		string.append("[name=");
		string.append(name);
		string.append(", directory=");
		string.append(directory);
		string.append(", group=");
		string.append(new SimpleDateFormat(GROUP_FORMAT).format(snapshotDate));
		string.append(", snapshotDate=");
		string.append(new SimpleDateFormat(FILE_NAME_DATE_FORMAT).format(snapshotDate));
		string.append(", part=");
		string.append(part);
		string.append(", partTotal=");
		string.append(partTotal);
		string.append("]");
		return string.toString();
	}

	public CompaniesFileMetaData(String name, String directory, String group, int part, int partTotal, Date snapshotDate) {
		super();
		this.name = name;
		this.directory = directory;
		this.group = group;
		this.part = part;
		this.partTotal = partTotal;
		this.snapshotDate = snapshotDate;
	}

	public String getName() {
		return name;
	}

	public String getDirectory() {
		return directory;
	}

	public String getGroup() {
		return group;
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
