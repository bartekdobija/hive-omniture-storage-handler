package com.github.bartekdobija.omniture.serde;

import com.github.bartekdobija.omniture.metadata.ColumnType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.junit.Test;

import java.util.Properties;

import static com.github.bartekdobija.omniture.handler.OmnitureStorageHandler.*;
import static org.junit.Assert.*;

public class OmnitureSerDeTest {

  @Test
  public void core() {
    assertEquals(ColumnType.STRING, ColumnType.valueOf("STRING"));
  }

  @Test
  public void initialize() throws SerDeException {
    OmnitureSerDe serde = new OmnitureSerDe();

    Configuration conf = new Configuration();

    Properties props = new Properties();
    props.put(MANIFEST_FILE_KEY,
        getClass().getResource("/data/suiteid_2015-02-10.txt").toString());

    serde.initialize(conf, props);
  }

}
