package com.cloudera.companies.core.common.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompaniesFileKey implements WritableComparable<CompaniesFileKey> {

	private String name;
	private String group;

	public CompaniesFileKey() {
	}

	public CompaniesFileKey(String group, String name) {
		this.group = group;
		this.name = name;
	}

	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		string.append("[group=");
		string.append(group);
		string.append(", name=");
		string.append(name);
		string.append("]");
		return string.toString();
	}

	@Override
	public int hashCode() {
		return (group + name).hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CompaniesFileKey) {
			CompaniesFileKey that = (CompaniesFileKey) obj;
			return group.equals(that.group) && name.equals(that.name);
		}
		return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		group = WritableUtils.readString(in);
		name = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, group);
		WritableUtils.writeString(out, name);
	}

	@Override
	public int compareTo(CompaniesFileKey that) {
		int result = group.compareTo(that.group);
		return result == 0 ? name.compareTo(that.name) : result;
	}

	public String getName() {
		return name;
	}

	public String getGroup() {
		return group;
	}

}
