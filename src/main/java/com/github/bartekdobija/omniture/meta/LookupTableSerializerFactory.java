package com.github.bartekdobija.omniture.meta;

public class LookupTableSerializerFactory {

  public static final LookupTableSerializer defaultLookupTableSerializer =
      new JsonLookupTableSerializer();

  public static final String DEFAULT_SERIALIZER =
      defaultLookupTableSerializer.getClass().getName();

  public static LookupTableSerializer create(String clazz)
      throws ReflectiveOperationException {
    if (clazz == null || clazz.isEmpty()) {
      throw new ReflectiveOperationException("null parameter passed to the factory");
    }
    if (clazz.equals(DEFAULT_SERIALIZER)) {
      return defaultLookupTableSerializer;
    }
    return (LookupTableSerializer) Class.forName(clazz).newInstance();
  }
}
