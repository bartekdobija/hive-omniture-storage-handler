package com.github.bartekdobija.omniture.handler;

import com.github.bartekdobija.omniture.meta.OmnitureHiveMetaHook;
import com.github.bartekdobija.omniture.metadata.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.*;

public class OmnitureStorageHandler
    extends DefaultStorageHandler implements HiveStoragePredicateHandler {

  public static final String MANIFEST_FILE_KEY = "manifest.file";
  /* (optional) manual configuration */
  public static final String METADATA_HEADER_KEY = "metadata.header";
  public static final String LOOKUP_TABLE_KEY = "metadata.lookuptable";
  public static final String LOOKUP_ENABLED_KEY = "metadata.lookuptable.enabled";
  public static final String DATA_FILES_KEY = "data.files";

  public static final String DATAFILE_SEPARATOR = ",";
  public static final String COLTYPE_SEPARATOR = ":";

  public static final Log LOG = LogFactory.getLog(
      DefaultStorageHandler.class.getName());

  @Override
  public void configureInputJobProperties(
      TableDesc tableDesc, Map<String, String> jobProperties){
    jobConfig(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(
      TableDesc tableDesc, Map<String, String> jobProperties) {
    jobConfig(tableDesc, jobProperties);
  }

  public void configureTableJobProperties(
      TableDesc tableDesc, Map<String, String> jobProperties) {
    jobConfig(tableDesc, jobProperties);
  }

  private void jobConfig(TableDesc tableDesc, Map<String, String> jobProps) {
    Properties props = tableDesc.getProperties();
    String manifest = props.getProperty(MANIFEST_FILE_KEY);

    if (manifest != null) {
      try {
        jobProps.put(INPUT_DIR,
            dataFiles(new OmnitureMetadataFactory().create(manifest)));
      } catch (MetadataException ex) {
        ex.printStackTrace();
      }
    } else {
      jobProps.put(INPUT_DIR, props.getProperty(DATA_FILES_KEY));
    }
  }

  private String dataFiles(final OmnitureMetadata meta) {
    List<String> paths = new ArrayList<>();
    for(DataFile file: meta.getDataFiles()) {
      paths.add(file.getName());
    }
    return StringUtils.join(DATAFILE_SEPARATOR, paths);
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf,
                                                Deserializer deserializer,
                                                ExprNodeDesc exprNodeDesc) {
    return null;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return OmnitureInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return OmnitureOutputFormat.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return new OmnitureHiveMetaHook();
  }
}
