package com.github.bartekdobija.omniture.handler;

import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class OmnitureInputFormat extends TextInputFormat {

  @Override
  public RecordReader getRecordReader(InputSplit inputSplit,
                                      JobConf jobConf,
                                      Reporter reporter) throws IOException {
    return super.getRecordReader(inputSplit, jobConf, reporter);
  }
}
