package com.github.bartekdobija.omniture.serde;


import com.github.bartekdobija.omniture.metadata.*;
import com.github.bartekdobija.omniture.row.DenormalizedDataRowParser;
import com.github.bartekdobija.omniture.row.RowParser;
import com.github.bartekdobija.omniture.row.RowParserException;
import com.github.bartekdobija.omniture.serde.utils.OmnitureSerDeUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.github.bartekdobija.omniture.handler.OmnitureStorageHandler.*;

public class OmnitureSerDe extends AbstractSerDe {

  private ObjectInspector inspector;
  private LookupTableIndex lookup;
  private RowParser parser;

  public static final Log LOG = LogFactory.getLog(
      OmnitureSerDe.class.getName());

  @Override
  public void initialize(@Nullable Configuration conf, Properties tblProps)
      throws SerDeException {

    if(LOG.isDebugEnabled()) {
      LOG.debug("table properties: " + tblProps);
    }

    String manifest = tblProps.getProperty(MANIFEST_FILE_KEY);

    List<Column> cols;
    try {
      OmnitureMetadata meta = new OmnitureMetadataFactory().create(manifest);
      lookup = meta.getLookupTable().getIndex();
      cols = meta.getHeader().getColumns();
    } catch (MetadataException e) {
      throw new SerDeException(e);
    }

    List<String> colNames = new ArrayList<>(cols.size());
    List<ObjectInspector> objInspectors = new ArrayList<>(cols.size());

    for (Column c : cols) {
      colNames.add(c.getName());
      objInspectors.add(OmnitureSerDeUtils.toObjectInspector(c.getType()));
    }

    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        colNames, objInspectors);

    try {
      parser = DenormalizedDataRowParser.newInstance(cols, lookup);
    } catch (RowParserException e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return null;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector)
      throws SerDeException {
    new NotImplementedException(
        "omniture table serialization is not supported");
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    return IteratorUtils.toList(
        parser.parse(((Text) writable).toString()).iterator());
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

}
