package com.github.bartekdobija.omniture.serde.utils;

import com.github.bartekdobija.omniture.metadata.ColumnType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class OmnitureSerDeUtils {

  public static ObjectInspector toObjectInspector(final ColumnType type) {
    if (type == null) {
      return null;
    }
    return toObjectInspector(type.name);
  }

  public static ObjectInspector toObjectInspector(final String name) {
    if (name == null) {
      return null;
    }

    if (name.equals(ColumnType.INT.name)) {
      return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    } else if (name.equals(ColumnType.STRING.name)) {
      return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    } else if (name.equals(ColumnType.LONG.name)
        || name.equals(ColumnType.BIGINT.name)) {
      return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    } else if (name.equals(ColumnType.FLOAT.name)) {
      return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
    } else if (name.equals(ColumnType.DOUBLE.name)) {
      return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    } else if (name.equals(ColumnType.STRING_ARRAY.name)) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    } else if (name.equals(ColumnType.INT_ARRAY.name)) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    } else if (name.equals(ColumnType.LONG_ARRAY.name)) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    } else if (name.equals(ColumnType.FLOAT_ARRAY.name)) {
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
    } else if (name.equals(ColumnType.TIMESTAMP.name)) {
      return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
    } else if (name.equals(ColumnType.BOOLEAN.name)) {
      return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    } else if (name.equals(ColumnType.STRING_MAP.name)) {
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    } else if (name.equals(ColumnType.INT_MAP.name)) {
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    } else if (name.equals(ColumnType.IS_MAP.name)) {
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

}
