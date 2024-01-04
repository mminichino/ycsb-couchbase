package site.ycsb.db.couchbase3;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Collected metric values.
 */
public class MetricValue {
  private final double first;
  private final double second;

  public MetricValue(JsonObject block) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      double first_val = Double.parseDouble(values.get(0).getAsJsonArray().get(1).getAsString());
      double second_val = Double.parseDouble(values.get(1).getAsJsonArray().get(1).getAsString());
      this.first = Double.isNaN(first_val) || Double.isInfinite(first_val) ? 0 : first_val;
      this.second = Double.isNaN(second_val) || Double.isInfinite(second_val) ? 0 : second_val;
  }

  public long getLong() {
      return Math.round(this.second);
  }

  public double getDouble() {
      return this.second;
  }

  public double getDelta() {
      return this.second - this.first;
  }

  public long getLongDelta() {
      return Math.round(getDelta());
  }
}
