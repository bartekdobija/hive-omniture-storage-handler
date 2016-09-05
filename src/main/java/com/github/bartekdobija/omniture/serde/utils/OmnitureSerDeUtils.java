package com.github.bartekdobija.omniture.serde.utils;

import com.github.bartekdobija.omniture.metadata.ColumnType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class OmnitureSerDeUtils {

  public static ObjectInspector toObjectInspector(ColumnType type) {
    if (type == null) {
      return null;
    }

    if (type == ColumnType.INT) {
      return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    } else if (type == ColumnType.STRING) {
      return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    } else if (type == ColumnType.LONG || type == ColumnType.BIGINT) {
      return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    } else if (type == ColumnType.FLOAT) {
      return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
    } else if (type == ColumnType.DOUBLE) {
      return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    } else if (type == ColumnType.STRING_ARRAY) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    } else if (type == ColumnType.INT_ARRAY) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    } else if (type == ColumnType.LONG_ARRAY) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    } else if (type == ColumnType.FLOAT_ARRAY) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
    } else if (type == ColumnType.TIMESTAMP) {
      return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
    } else if (type == ColumnType.BOOLEAN) {
      return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    } else if (type == ColumnType.STRING_MAP) {
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    } else if (type == ColumnType.INT_MAP) {
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    } else if (type == ColumnType.IS_MAP) {
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

}
