package com.github.bartekdobija.omniture.handler;


import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;
import static org.junit.Assert.*;
import static com.github.bartekdobija.omniture.handler.OmnitureStorageHandler.*;

public class OmnitureStorageHandlerTest {

  @Test
  public void manifestBasedInit() {

    OmnitureStorageHandler handler = new OmnitureStorageHandler();

    Properties props = new Properties();
    props.put(MANIFEST_FILE_KEY,
        getClass().getResource("/data/suiteid_2015-02-10.txt").toString());

    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(props);

    Map<String, String> jobConf = new HashMap<>();

    handler.configureInputJobProperties(tableDesc, jobConf);

    assertTrue(jobConf.get(INPUT_DIR).contains("01-suiteid"));
    assertTrue(jobConf.get(INPUT_DIR).contains("02-suiteid"));

  }
}
