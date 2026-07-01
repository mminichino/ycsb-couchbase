package com.codelry.util.ycsb.couchbase;

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.codec.TypeRef;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.DecodingFailureException;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.codelry.util.ycsb.ByteIterator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MapTranscoder implements Transcoder {

  public static MapTranscoder INSTANCE = new MapTranscoder();

  private MapTranscoder() { }

  @Override
  public EncodedValue encode(final Object input) {
    if (input instanceof CommonOptions.BuiltCommonOptions || input instanceof CommonOptions) {
      throw InvalidArgumentException.fromMessage("No content provided, cannot " +
          "encode " + input.getClass().getSimpleName() + " as content!");
    }

    if (input instanceof byte[]) {
      return new EncodedValue((byte[]) input, CodecFlags.JSON_COMPAT_FLAGS);
    } else if (input instanceof Map) {
      try {
        return new EncodedValue(createMapper().writeValueAsBytes(input), CodecFlags.JSON_COMPAT_FLAGS);
      } catch (Throwable t) {
        throw new EncodingFailureException("Serializing of content + " + redactUser(input) + " to JSON failed.", t);
      }
    } else {
      throw InvalidArgumentException.fromMessage("Only byte[] and Map types are supported for the MapTranscoder!");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T decode(final Class<T> target, final byte[] input, int flags) {
    if (target.equals(byte[].class)) {
      return (T) input;
    } else if (target.equals(Map.class) || target.equals(HashMap.class)) {
      TypeReference<Map<String, ByteIterator>> typeRef = new TypeReference<>() {};
      try {
        return (T) createMapper().readValue(new String(input, StandardCharsets.UTF_8), typeRef);
      } catch (Throwable e) {
        throw new DecodingFailureException(e);
      }
    } else {
      throw new DecodingFailureException("MapTranscoder can only decode into either byte[] or Map!");
    }
  }

  @Override
  public <T> T decode(final TypeRef<T> target, final byte[] input, final int flags) {
    if (target.type().equals(byte[].class)) {
      return (T) input;
    }
    try {
      ObjectMapper mapper = createMapper();
      return mapper.readValue(input, mapper.constructType(target.type()));
    } catch (Throwable e) {
      throw new DecodingFailureException(e);
    }
  }

  private ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ByteIteratorCodec");
    module.addSerializer(ByteIterator.class, new ByteIteratorSerializer());
    module.addDeserializer(ByteIterator.class, new ByteIteratorDeserializer());
    mapper.registerModule(module);
    return mapper;
  }
}
