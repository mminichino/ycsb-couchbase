package site.ycsb.db.couchbase3;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Collected metric values.
 */
public class MetricValue {
  private double previous = 0.0;
  private long previousTimestamp = 0;
  private double current = 0.0;
  private long currentTimestamp = 0;

  public MetricValue() {
  }

  public void setValue(JsonObject block) {
    double value = 0.0;
    long timestamp = 0;

    if (block.has("data")) {
      JsonArray data = block.get("data").getAsJsonArray();
      if (!data.isEmpty()) {
        JsonArray values = data.get(0).getAsJsonObject().get("values").getAsJsonArray();
        timestamp = Long.parseLong(values.get(0).getAsJsonArray().get(0).getAsString());
        value = Double.parseDouble(values.get(0).getAsJsonArray().get(1).getAsString());
        value = Double.isNaN(value) ? 0 : Double.isInfinite(value) ? Double.MAX_VALUE : Math.abs(value);
      }
    }

    this.previous = this.current > 0 ? this.current : value;
    this.current = value;
    this.previousTimestamp = this.currentTimestamp > 0 ? this.currentTimestamp : timestamp;
    this.currentTimestamp = timestamp;
  }

  public double getValue() {
    return this.current;
  }

  public double getDelta() {
    if (this.current >= this.previous) {
      return this.current - this.previous;
    } else {
      return 0.0;
    }
  }

  public long getTimeDelta() {
    if (this.currentTimestamp >= this.previousTimestamp) {
      return this.currentTimestamp - this.previousTimestamp;
    } else {
      return 0;
    }
  }

  public double getPerSec() {
    long timeDelta = getTimeDelta();
    if (timeDelta > 0) {
      double diff = getDelta();
      return diff / timeDelta;
    } else {
      return 0.0;
    }
  }
}
