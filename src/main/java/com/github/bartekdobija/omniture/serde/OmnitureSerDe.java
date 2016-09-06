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
import java.util.*;

import static com.github.bartekdobija.omniture.handler.OmnitureStorageHandler.*;

public class OmnitureSerDe extends AbstractSerDe {

  private ObjectInspector inspector;
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
    LookupTableIndex lookup;
    List<String> colNames;
    List<ObjectInspector> objInspectors;
    if (manifest != null) {
      try {
        OmnitureMetadata meta = new OmnitureMetadataFactory().create(manifest);
        if("false".equalsIgnoreCase(tblProps.getProperty(LOOKUP_ENABLED_KEY))) {
          lookup = new LookupTableIndex(); //empty lookup table
        } else {
          lookup = meta.getLookupTable().getIndex();
        }
        cols = meta.getHeader().getColumns();
      } catch (MetadataException e) {
        throw new SerDeException(e);
      }

      colNames = new ArrayList<>(cols.size());
      objInspectors = new ArrayList<>(cols.size());

      for (Column c : cols) {
        colNames.add(c.getName());
        objInspectors.add(OmnitureSerDeUtils.toObjectInspector(c.getType()));
      }
    } else {
      String[] header =
          tblProps.getProperty(METADATA_HEADER_KEY).split(DATAFILE_SEPARATOR);

      colNames = new ArrayList<>(header.length);
      objInspectors = new ArrayList<>(header.length);
      cols = new ArrayList<>(header.length);

      ColumnType[] types = ColumnType.values();
      for (String s : header) {
        String[] col = s.split(COLTYPE_SEPARATOR);
        if (col.length != 2) {
          throw new SerDeException("invalid header format");
        }
        colNames.add(col[0]);
        objInspectors.add(OmnitureSerDeUtils.toObjectInspector(col[1]));
        for (ColumnType t : types) {
          if(t.name.equalsIgnoreCase(col[1])) {
            cols.add(new Column(col[0], t));
            break;
          }
        }
      }
      // lookup index is not supported in the manual mode
      lookup = new LookupTableIndex();
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
