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

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.*;

public class OmnitureStorageHandler
    extends DefaultStorageHandler implements HiveStoragePredicateHandler {

  public static final String MANIFEST_FILE_KEY = "omniture.manifest.file";
  /* (optional) manual configuration */
  public static final String METADATA_HEADER_KEY = "omniture.metadata.header";
  public static final String LOOKUP_TABLE_KEY = "omniture.metadata.lookuptable";
  public static final String DATA_FILES_KEY = "omniture.data.files";
  public static final String LOOKUP_TABLE_SERIALIZER_KEY =
      "omniture.metadata.lookuptable.serializer";

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
    try {
      OmnitureMetadata metadata = new OmnitureMetadataFactory().create(
          tableDesc.getProperties().getProperty(MANIFEST_FILE_KEY));
      jobProps.put(INPUT_DIR, dataFiles(metadata));
    } catch (MetadataException ex) {
      ex.printStackTrace();
    }
  }

  private String dataFiles(OmnitureMetadata meta) {
    List<String> paths = new ArrayList<>();
    for(DataFile file: meta.getDataFiles()) {
      paths.add(file.getName());
    }
    return StringUtils.join(DATAFILE_SEPARATOR, paths);
  }

  private String headerDefinitions(OmnitureMetadata meta)
      throws MetadataException {
    List<String> entries = new ArrayList<>();
    for(Column c: meta.getHeader().getColumns()) {
      entries.add(c.getName() + COLTYPE_SEPARATOR + c.getType());
    }
    return StringUtils.join(DATAFILE_SEPARATOR, entries);
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
