package site.ycsb.db.couchbase3;

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.Transcoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

public class JacksonTranscoder implements Transcoder {

  public static JacksonTranscoder INSTANCE = new JacksonTranscoder();

  private JacksonTranscoder() { }

  @Override
  public EncodedValue encode(final Object input) {
    if (input instanceof CommonOptions.BuiltCommonOptions || input instanceof CommonOptions) {
      throw InvalidArgumentException.fromMessage("No content provided, cannot " +
          "encode " + input.getClass().getSimpleName() + " as content!");
    }

    if (input instanceof byte[]) {
      return new EncodedValue((byte[]) input, CodecFlags.JSON_COMPAT_FLAGS);
    } else if (input instanceof JsonNode) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectWriter writer = mapper.writer();
      try {
        return new EncodedValue(writer.writeValueAsBytes(input), CodecFlags.JSON_COMPAT_FLAGS);
      } catch (Throwable t) {
        throw new EncodingFailureException("Serializing of content + " + redactUser(input) + " to JSON failed.", t);
      }
    } else {
      throw InvalidArgumentException.fromMessage("Only byte[] and JsonNode types are supported for the JacksonTranscoder!");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T decode(final Class<T> target, final byte[] input, int flags) {
    if (target.equals(byte[].class)) {
      return (T) input;
    } else if (target.equals(JsonNode.class) || target.equals(HashMap.class)) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectReader reader = mapper.reader();
      try {
        return (T) reader.readTree(new String(input, StandardCharsets.UTF_8));
      } catch (Throwable e) {
        throw new DecodingFailureException(e);
      }
    } else {
      throw new DecodingFailureException("JacksonTranscoder can only decode into either byte[] or JsonNode!");
    }
  }
}
