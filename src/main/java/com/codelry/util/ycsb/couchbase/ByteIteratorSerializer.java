package com.codelry.util.ycsb.couchbase;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.codelry.util.ycsb.ByteIterator;

import java.io.IOException;

public class ByteIteratorSerializer extends JsonSerializer<ByteIterator> {
  @Override
  public void serialize(ByteIterator value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
    gen.writeString(value.toString());
  }
}
