package site.ycsb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.ObjectUtils;

public class Record {
  private final ObjectNode data;

  public Record() {
    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
  }

  public void add(String field, int value) {
    this.data.put(field, value);
  }

  public void add(String field, Integer value) {
    this.data.put(field, value);
  }

  public void add(String field, float value) {
    this.data.put(field, value);
  }

  public void add(String field, Float value) {
    this.data.put(field, value);
  }

  public void add(String field, double value) {
    this.data.put(field, value);
  }

  public void add(String field, Double value) {
    this.data.put(field, value);
  }

  public void add(String field, String value) {
    this.data.put(field, value);
  }

  public void add(String field) {
    this.data.putNull(field);
  }

  public ObjectNode contents() {
    return this.data;
  }
}
