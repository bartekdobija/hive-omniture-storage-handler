package com.github.bartekdobija.omniture.meta;

import java.util.Map;

public interface LookupTableSerializer {

  String serialize(Map<String, String> obj);
  Map<String, String> deserialize(String str);

}
