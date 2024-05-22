package site.ycsb.db.couchbase3;

import com.couchbase.client.core.error.InvalidArgumentException;

import com.fasterxml.jackson.databind.ObjectMapper;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;


import static java.util.Objects.requireNonNull;

public class MapObject implements Serializable {
  private static final long serialVersionUID = 200L;
  private final Map<String, Map<String, ByteIterator>> content;

  private MapObject() {
    content = new HashMap<>();
  }

  private MapObject(int initialCapacity) {
    content = new HashMap<>(initialCapacity);
  }

  public static MapObject create() {
    return new MapObject();
  }

  public static MapObject create(int initialCapacity) {
    return new MapObject(initialCapacity);
  }

  public static MapObject from(final Map<String, Map<String, String>> mapData) {
    if (mapData == null) {
      throw new NullPointerException("Null input Map unsupported");
    }

    MapObject result = new MapObject(mapData.size());
    try {
      mapData.forEach((key, value) -> {
        requireNonNull(key, "The key is not allowed to be null");
        result.put(key, new HashMap<>());
        value.forEach((k, v) -> {
          result.get(key).put(k, new StringByteIterator(v));
        });
      });
    } catch (ClassCastException e) {
      throw InvalidArgumentException.fromMessage("Map key must be String", e);
    }

    return result;
  }

  public static MapObject fromJson(final String s) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(s, MapObject.class);
    } catch (Exception e) {
      throw InvalidArgumentException.fromMessage("Cannot convert string to JsonObject", e);
    }
  }

  public MapObject fromJson(final byte[] s) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(s, MapObject.class);
    } catch (Exception e) {
      throw InvalidArgumentException.fromMessage("Cannot convert byte array to JsonObject", e);
    }
  }

  public MapObject put(final String name, final Map<String, ByteIterator> value) {
    content.put(name, value);
    return this;
  }

  public Map<String, ByteIterator> get(final String name) {
    return this.content.get(name);
  }

  public HashMap<String, ByteIterator> toHashMap(String name) {
    return new HashMap<>(this.content.get(name));
  }
}
