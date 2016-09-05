package com.github.bartekdobija.omniture.meta;

import com.github.bartekdobija.omniture.handler.OmnitureStorageHandler;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Map;

public class OmnitureHiveMetaHook implements HiveMetaHook {

  public static final String NATIVE_TABLE_SUPPORT_EXCEPTION =
      "Managed tables are not supported by this storage handler";
  public static final  String INCOMPLETE_TABLE_CONFIG_EXCEPTION =
      "Incomplete table configuration";

  @Override
  public void preCreateTable(Table table) throws MetaException {
    if (!MetaStoreUtils.isExternalTable(table)) {
      throw new MetaException(NATIVE_TABLE_SUPPORT_EXCEPTION);
    }

    Map<String, String> params = table.getParameters();
    if(params.get(OmnitureStorageHandler.MANIFEST_FILE_KEY) == null
        && (params.get(OmnitureStorageHandler.METADATA_HEADER_KEY) == null
            || params.get(OmnitureStorageHandler.DATA_FILES_KEY) == null)) {
      throw new MetaException(INCOMPLETE_TABLE_CONFIG_EXCEPTION);
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {

  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {

  }

  @Override
  public void preDropTable(Table table) throws MetaException {

  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {

  }

  @Override
  public void commitDropTable(Table table, boolean b) throws MetaException {

  }
}
