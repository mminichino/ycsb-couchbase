package site.ycsb.db.couchbase3;

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.Transcoder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.DecodingFailureException;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MapTranscoder implements Transcoder {

  public static MapTranscoder INSTANCE = new MapTranscoder();

  private MapTranscoder() { }

  @Override
  @SuppressWarnings("unchecked")
  public EncodedValue encode(final Object input) {
    if (input instanceof CommonOptions.BuiltCommonOptions || input instanceof CommonOptions) {
      throw InvalidArgumentException.fromMessage("No content provided, cannot " +
          "encode " + input.getClass().getSimpleName() + " as content!");
    }

    if (input instanceof byte[]) {
      return new EncodedValue((byte[]) input, CodecFlags.JSON_COMPAT_FLAGS);
    } else if (input instanceof Map) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode document = mapper.createObjectNode();
      ((HashMap<String, ByteIterator>) input).entrySet()
          .parallelStream()
          .forEach(entry -> document.put(entry.getKey(), entry.getValue().toString()));
      try {
        return new EncodedValue(mapper.writeValueAsBytes(document), CodecFlags.JSON_COMPAT_FLAGS);
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
      ObjectMapper mapper = new ObjectMapper();
      TypeReference<HashMap<String, String>> typeRef = new TypeReference<>() {};
      Map<String, ByteIterator> result = new HashMap<>();
      try {
        mapper.readValue(new String(input, StandardCharsets.UTF_8), typeRef).entrySet()
            .parallelStream()
            .forEach(entry -> result.put(entry.getKey(), new StringByteIterator(entry.getValue())));
        return (T) result;
      } catch (IOException e) {
        throw new DecodingFailureException(e);
      }
    } else {
      throw new DecodingFailureException("MapTranscoder can only decode into either byte[] or Map!");
    }
  }
}
