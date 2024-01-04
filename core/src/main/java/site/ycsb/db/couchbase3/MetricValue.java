package site.ycsb.db.couchbase3;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.text.DecimalFormat;

/**
 * Collected metric values.
 */
public class MetricValue {
  private final double first;
  private final double second;
  private final MetricType type;
  private final boolean decimal;
  private final String transform;
  private final String label;

  public MetricValue(JsonObject block, MetricType type, boolean decimal, String transform, String label) {
    JsonArray data = block.get("data").getAsJsonArray();

    if (!data.isEmpty()) {
      JsonArray values = data.get(0).getAsJsonObject().get("values").getAsJsonArray();
      double first_val = Double.parseDouble(values.get(0).getAsJsonArray().get(1).getAsString());
      double second_val = Double.parseDouble(values.get(1).getAsJsonArray().get(1).getAsString());
      this.first = Double.isNaN(first_val) ? 0 : Double.isInfinite(first_val) ? Double.MAX_VALUE : first_val;
      this.second = Double.isNaN(second_val) ? 0 : Double.isInfinite(second_val) ? Double.MAX_VALUE : second_val;
    } else {
      this.first = 0.0;
      this.second = 0.0;
    }

    this.type = type;
    this.decimal = decimal;
    this.transform = transform;
    this.label = label;
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

  public String generate() {
    double finalValue;
    if (type == MetricType.COUNTER) {
      finalValue = getDelta();
    } else {
      finalValue = getDouble();
    }

    switch (transform) {
      case "to_gib":
        return formatDataSize(finalValue);
      case "from_ns":
        return fromNanoSeconds(finalValue);
      case "inverse":
        return percentInv(finalValue);
      case "comma_delim":
        return commaDelimit(finalValue);
      case "to_ms":
        return toMilliSeconds(finalValue);
      default:
        return defaultFormat(finalValue);
    }
  }

  private String defaultFormat(double value) {
    if (decimal) {
      return String.format("%s: %.2f ", label, value);
    } else {
      return String.format("%s: %d ", label, Math.round(value));
    }
  }

  private String toMilliSeconds(double seconds) {
    double value = seconds * 1000;
    if (decimal) {
      return String.format("%s: %.2f ms ", label, value);
    } else {
      return String.format("%s: %d ms ", label, Math.round(value));
    }
  }

  private String commaDelimit(double value) {
    DecimalFormat formatter;
    if (decimal) {
      formatter = new DecimalFormat("#,###.##");
    } else {
      formatter = new DecimalFormat("#,###");
    }
    return String.format("%s: %s ", label, formatter.format(value));
  }

  private String percentInv(double percentage) {
    double value = 100 - percentage;
    if (decimal) {
      return String.format("%s: %.2f%% ", label, value);
    } else {
      return String.format("%s: %d%% ", label, Math.round(value));
    }
  }

  private String fromNanoSeconds(double nano) {
    double value = nano / 1000000;
    if (decimal) {
      return String.format("%s: %.2f ms ", label, value);
    } else {
      return String.format("%s: %d ms ", label, Math.round(value));
    }
  }

  private String formatDataSize(double bytes) {
    String output;

    double k = bytes / 1024.0;
    double m = ((bytes / 1024.0) / 1024.0);
    double g = (((bytes / 1024.0) / 1024.0) / 1024.0);
    double t = ((((bytes / 1024.0) / 1024.0) / 1024.0) / 1024.0);

    if (t > 1) {
      output = Math.round(t) + " TB";
    } else if (g > 1) {
      output = Math.round(g) + " GB";
    } else if (m > 1) {
      output = Math.round(m) + " MB";
    } else if (k > 1) {
      output = Math.round(k) + " KB";
    } else {
      output = Math.round(bytes) + " B";
    }

    return String.format("%s: %s ", label, output);
  }
}
