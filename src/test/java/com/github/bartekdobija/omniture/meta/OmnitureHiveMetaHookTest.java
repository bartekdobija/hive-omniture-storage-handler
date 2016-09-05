package com.github.bartekdobija.omniture.meta;


import com.github.bartekdobija.omniture.handler.OmnitureStorageHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Test;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class OmnitureHiveMetaHookTest {

  private OmnitureHiveMetaHook hook = new OmnitureHiveMetaHook();
  private HashMap<String, String> configBase = new HashMap<String, String>(){
    {
      put("EXTERNAL", "TRUE");
    }
  };

  @Test
  public void validManifestBasedPreCreate() throws MetaException {
    Table table = new Table();
    Map<String, String> conf = (HashMap) configBase.clone();
    conf.put(OmnitureStorageHandler.MANIFEST_FILE_KEY, "test");

    table.setParameters(conf);

    hook.preCreateTable(table);
  }

  @Test
  public void validStaticConfigPreCreate() throws MetaException {
    Table table = new Table();
    Map<String, String> conf = (HashMap) configBase.clone();
    conf.put(OmnitureStorageHandler.METADATA_HEADER_KEY, "test");
    conf.put(OmnitureStorageHandler.DATA_FILES_KEY, "test");

    table.setParameters(conf);

    hook.preCreateTable(table);
  }

  @Test(expected = MetaException.class)
  public void missingStaticDataFilesConfig() throws MetaException {
    Table table = new Table();
    Map<String, String> conf = (HashMap) configBase.clone();
    conf.put(OmnitureStorageHandler.DATA_FILES_KEY, "path");

    table.setParameters(conf);

    hook.preCreateTable(table);
  }

  @Test(expected = MetaException.class)
  public void missingStaticHeadersConfig() throws MetaException {
    Table table = new Table();
    Map<String, String> conf = (HashMap) configBase.clone();
    conf.put(OmnitureStorageHandler.METADATA_HEADER_KEY, "path");

    table.setParameters(conf);

    hook.preCreateTable(table);
  }

  @Test(expected = MetaException.class)
  public void managedTable() throws MetaException {
    Table table = new Table();
    Map<String, String> conf = (HashMap) configBase.clone();
    conf.put("EXTERNAL", "FALSE");

    table.setParameters(conf);

    hook.preCreateTable(table);
  }

  @Test
  public void emptyMethods() throws MetaException {
    Table table = new Table();
    hook.rollbackCreateTable(table);
    hook.commitCreateTable(table);
    hook.preDropTable(table);
    hook.rollbackDropTable(table);
    hook.commitDropTable(table, false);
  }

}
