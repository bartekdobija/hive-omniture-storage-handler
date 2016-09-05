package com.github.bartekdobija.omniture.meta;

import com.github.bartekdobija.omniture.metadata.LookupTableIndex;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class JsonLookupTableSerializerTest {

  private JsonLookupTableSerializer serializer =
      new JsonLookupTableSerializer();
  private String subject = "{\"a:b\":\"c\",\"d:e\":\"f\"}";
  private String invalidSubject = "{\"a:b\":\"c\",";

  @Test
  public void serialize() {

    LookupTableIndex index = new LookupTableIndex();
    index.setGroupValue("a","b","c");
    index.setGroupValue("d","e","f");

    String result = serializer.serialize(index);

    assertNotNull(result);
    assertTrue(result.contains("a:b"));
    assertTrue(result.contains("d:e"));
    assertEquals("null",serializer.serialize(null));
  }

  @Test
  public void deserialize() {
    Map<String, String> result = serializer.deserialize(subject);

    assertNotNull(result);
    assertTrue(result.toString().contains("a:b"));
    assertTrue(result.toString().contains("c"));
    assertNull(serializer.deserialize(null));
    assertNull(serializer.deserialize(""));
    assertNull(serializer.deserialize(invalidSubject));
  }

}
