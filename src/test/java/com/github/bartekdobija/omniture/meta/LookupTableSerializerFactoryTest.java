package com.github.bartekdobija.omniture.meta;

import org.junit.Test;

import static org.junit.Assert.*;

public class LookupTableSerializerFactoryTest {

  private static final String serialed = "{\"a:b\":\"c\"}";

  @Test
  public void init() throws ReflectiveOperationException {
    LookupTableSerializer serial = LookupTableSerializerFactory.create(
        JsonLookupTableSerializer.class.getName());

    assertNotNull(serial);
    assertTrue(serial instanceof JsonLookupTableSerializer);
  }

}
