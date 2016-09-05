package com.github.bartekdobija.omniture.handler;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class OmnitureOutputFormat extends TextOutputFormat {

  @Override
  public RecordWriter getRecordWriter(
      FileSystem fs, JobConf jobConf, String s, Progressable progress)
      throws IOException {
    return super.getRecordWriter(fs, jobConf, s, progress);
  }
}
