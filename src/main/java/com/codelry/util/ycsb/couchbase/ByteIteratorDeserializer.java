package com.codelry.util.ycsb.couchbase;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.StringByteIterator;

import java.io.IOException;

public class ByteIteratorDeserializer extends JsonDeserializer<ByteIterator> {
  @Override
  public ByteIterator deserialize(JsonParser jp, DeserializationContext context) throws IOException {
    return new StringByteIterator(jp.readValueAs(String.class));
  }
}
