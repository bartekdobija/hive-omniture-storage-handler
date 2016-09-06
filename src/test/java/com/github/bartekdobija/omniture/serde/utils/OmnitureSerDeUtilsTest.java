package com.github.bartekdobija.omniture.serde.utils;

import com.github.bartekdobija.omniture.metadata.ColumnType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static org.junit.Assert.*;

import static com.github.bartekdobija.omniture.serde.utils.OmnitureSerDeUtils.*;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;

public class OmnitureSerDeUtilsTest {

  @Test
  public void objectInspector() {


    assertNull(toObjectInspector((String) null));

    assertEquals(javaIntObjectInspector, toObjectInspector(ColumnType.INT));

    assertEquals(javaStringObjectInspector,
        toObjectInspector(ColumnType.STRING));

    assertEquals(javaLongObjectInspector, toObjectInspector(ColumnType.LONG));

    assertEquals(javaLongObjectInspector, toObjectInspector(ColumnType.BIGINT));

    assertEquals(javaFloatObjectInspector, toObjectInspector(ColumnType.FLOAT));

    assertEquals(javaDoubleObjectInspector,
        toObjectInspector(ColumnType.DOUBLE));

    assertEquals(ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector),
        toObjectInspector(ColumnType.STRING_ARRAY));

    assertEquals(ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector),
        toObjectInspector(ColumnType.INT_ARRAY));

    assertEquals(ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaLongObjectInspector),
        toObjectInspector(ColumnType.LONG_ARRAY));

    assertEquals(ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaFloatObjectInspector),
        toObjectInspector(ColumnType.FLOAT_ARRAY));

    assertEquals(javaTimestampObjectInspector,
        toObjectInspector(ColumnType.TIMESTAMP));

    assertEquals(javaBooleanObjectInspector,
        toObjectInspector(ColumnType.BOOLEAN));

    assertEquals(ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector),
        toObjectInspector(ColumnType.STRING_MAP));

    assertEquals(ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector),
        toObjectInspector(ColumnType.INT_MAP));

    assertEquals(ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector),
        toObjectInspector(ColumnType.IS_MAP));
  }

}
