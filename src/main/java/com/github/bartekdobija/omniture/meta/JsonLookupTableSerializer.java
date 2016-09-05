package com.github.bartekdobija.omniture.meta;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.util.HashMap;
import java.util.Map;

public class JsonLookupTableSerializer implements LookupTableSerializer {

  private final Gson parser = new Gson();

  @Override
  public String serialize(Map<String, String> obj) {
    return parser.toJson(obj);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, String> deserialize(String str) {
    if(str == null || str.isEmpty()) {
      return null;
    }
    try {
      return parser.fromJson(str,HashMap.class);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }
}
