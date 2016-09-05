package com.github.bartekdobija.omniture.serde.utils;

import com.github.bartekdobija.omniture.metadata.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import static com.github.bartekdobija.omniture.serde.utils.OmnitureSerDeUtils.*;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;

public class OmnitureSerDeUtilsTest {

  @Test
  public void objectInspector() {
    Assert.assertEquals(
        javaStringObjectInspector, toObjectInspector(ColumnType.STRING));
    Assert.assertEquals(
        javaIntObjectInspector, toObjectInspector(ColumnType.INT));
  }

}
