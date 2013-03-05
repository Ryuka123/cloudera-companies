package com.cloudera.companies.core.ingest.seq.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.cloudera.companies.core.ingest.IngestUtil.Counter;

public class CompaniesFileKey implements WritableComparable<CompaniesFileKey> {

  private String name;
  private String group;
  private Counter type;

  public CompaniesFileKey() {
  }

  public CompaniesFileKey(Counter type, String group, String name) {
    this.type = type;
    this.group = group;
    this.name = name;
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder();
    string.append("[group=");
    string.append(group);
    string.append(", type=");
    string.append(type);
    string.append(", name=");
    string.append(name);
    string.append("]");
    return string.toString();
  }

  @Override
  public int hashCode() {
    return (type.toString() + group + name).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CompaniesFileKey) {
      CompaniesFileKey that = (CompaniesFileKey) obj;
      return type.equals(that.type) && group.equals(that.group) && name.equals(that.name);
    }
    return false;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = WritableUtils.readEnum(in, Counter.class);
    group = WritableUtils.readString(in);
    name = WritableUtils.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, type);
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

  public Counter getType() {
    return type;
  }

}
