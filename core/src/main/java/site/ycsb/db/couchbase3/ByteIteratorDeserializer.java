package site.ycsb.db.couchbase3;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import site.ycsb.ByteIterator;
import site.ycsb.LongByteIterator;
import site.ycsb.StringByteIterator;

import java.io.IOException;

public class ByteIteratorDeserializer extends JsonDeserializer<ByteIterator> {
  @Override
  public ByteIterator deserialize(JsonParser jp, DeserializationContext context) throws IOException {
    if (jp.getNumberType() != null && jp.getNumberType() == JsonParser.NumberType.LONG) {
      return new LongByteIterator(jp.readValueAs(Long.class));
    } else if (jp.getNumberType() != null && jp.getNumberType() == JsonParser.NumberType.INT) {
      return new LongByteIterator(jp.readValueAs(Long.class));
    } else {
      return new StringByteIterator(jp.readValueAs(String.class));
    }
  }
}
