package site.ycsb.db.couchbase3;

import com.couchbase.client.java.http.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.micrometer.core.instrument.util.IOUtils;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.RemoteStatistics;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Collects database statistics by API, and reports them when requested.
 */
public class CouchbaseCollect extends RemoteStatistics {

  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseCollect");
  protected static final ch.qos.logback.classic.Logger STATISTICS =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.statistics");
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  private static final String STATISTICS_CONFIG_FILE = "statistics.json";
  public static final String COUCHBASE_HOST = "couchbase.hostname";
  public static final String COUCHBASE_USER = "couchbase.username";
  public static final String COUCHBASE_PASSWORD = "couchbase.password";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SSL_MODE = "couchbase.sslMode";
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private ScheduledFuture<?> apiHandle = null;
  private final String dateFormat = "yy-MM-dd'T'HH:mm:ss";
  private final SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
  private static String hostname;
  private static String username;
  private static String password;
  private static String bucketName;
  private static boolean sslMode;
  private static JsonArray metricList;
  private static final List<MetricValue> resultList = new ArrayList<>();
  private static final Map<String, MetricValue> resultMatrix = new LinkedHashMap<>();

  /**
   * Metric Mode.
   */
  public enum MetricMode {
    SYSTEM, BUCKET, XDCR, DISK
  }

  @Override
  public void init(Properties props) {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        properties.load(propFile.openStream());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    properties.putAll(props);

    hostname = properties.getProperty(COUCHBASE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    username = properties.getProperty(COUCHBASE_USER, CouchbaseConnect.DEFAULT_USER);
    password = properties.getProperty(COUCHBASE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    sslMode = properties.getProperty(COUCHBASE_SSL_MODE, "false").equals("true");

    URL configFile = classloader.getResource(STATISTICS_CONFIG_FILE);
    try {
      if (configFile != null) {
        String configJson = IOUtils.toString(configFile.openStream(), StandardCharsets.UTF_8);
        Gson gson = new Gson();
        metricList = gson.fromJson(configJson, JsonArray.class);
        for (int i = 0; i < metricList.size(); i++) {
          resultList.add(new MetricValue());
        }
        for (JsonElement metric : metricList) {
          String name = metric.getAsJsonObject().get("metric").getAsString();
          resultMatrix.put(name, new MetricValue());
        }
      } else {
        throw new RuntimeException("Can not access statistics config file");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void startCollectionThread() {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseHttpClient client = dbBuilder.connect(hostname, username, password).ssl(sslMode).build().getHttpClient();
    JsonArray metricSet = new JsonArray();

    for (JsonElement metric : metricList) {
      String name = metric.getAsJsonObject().get("metric").getAsString();
      MetricMode mode = MetricMode.valueOf(metric.getAsJsonObject().get("mode").getAsString());
      MetricType type = MetricType.valueOf(metric.getAsJsonObject().get("type").getAsString());
      metricSet.add(addMetric(name, mode, type));
    }

    String metricRequestBody = metricSet.toString();

    STATISTICS.info(String.format("==== Begin Cluster Collection %s ====\n", timeStampFormat.format(new Date())));

    Runnable callApi = () -> {
      StringBuilder line = new StringBuilder();
      String timeStamp = timeStampFormat.format(new Date());
      line.append(String.format("%s ", timeStamp));

      try {
        String endpoint = "/pools/default/stats/range";
        HttpResponse response = client.post(
                HttpTarget.manager(),
                HttpPath.of(endpoint),
                HttpPostOptions.httpPostOptions()
                        .body(HttpBody.json(metricRequestBody)));
        Gson gson = new Gson();
        JsonArray data = gson.fromJson(response.contentAsString(), JsonArray.class);

        for (int i = 0; i < metricList.size(); i++) {
          JsonElement metric = metricList.get(i);
          JsonObject result = data.getAsJsonArray().get(i).getAsJsonObject();
          String name = metric.getAsJsonObject().get("metric").getAsString();
          resultMatrix.get(name).setValue(result);
        }

        for (int i = 0; i < metricList.size(); i++) {
          JsonElement metric = metricList.get(i);
          String name = metric.getAsJsonObject().get("metric").getAsString();
          MetricType type = MetricType.valueOf(metric.getAsJsonObject().get("type").getAsString());
          boolean decimal = metric.getAsJsonObject().get("decimal").getAsBoolean();
          boolean perSecond = metric.getAsJsonObject().get("perSecond").getAsBoolean();
          String transform = metric.getAsJsonObject().get("transform").getAsString();
          String label = metric.getAsJsonObject().get("label").getAsString();
          MetricValue metricValue = resultMatrix.get(name);
          line.append(generate(metricValue, type, decimal, perSecond, transform, label));
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        line.append(String.format("Error: %s", e.getMessage()));
      }

      STATISTICS.info(String.format("%s\n", line));
      line.delete(0, line.length());
    };

    System.err.println("Starting remote statistics thread...");
    apiHandle = scheduler.scheduleWithFixedDelay(callApi, 1, 10, SECONDS);
  }

  private static JsonObject addMetric(String name, MetricMode mode, MetricType type) {
    JsonObject block = new JsonObject();
    JsonArray metric = getMetricStruct(name, bucketName, mode);

    JsonArray applyFunctions = new JsonArray();
    if (type == MetricType.COUNTER) {
      applyFunctions.add("max");
    } else {
      applyFunctions.add("avg");
    }

    block.add("metric", metric);
    block.add("applyFunctions", applyFunctions);

    if (type == MetricType.COUNTER || type == MetricType.TOTAL) {
      block.addProperty("nodesAggregation", "sum");
    } else {
      block.addProperty("nodesAggregation", "avg");
    }

    block.addProperty("alignTimestamps", true);
    block.addProperty("step", 10);
    block.addProperty("start", -20);

    return block;
  }

  private static JsonArray getMetricStruct(String name, String bucketName, MetricMode mode) {
    JsonArray metric = new JsonArray();

    JsonObject definition = new JsonObject();
    definition.addProperty("label", "name");
    definition.addProperty("value", name);
    metric.add(definition);

    if (mode == MetricMode.BUCKET) {
      JsonObject bucketDefinition = new JsonObject();
      bucketDefinition.addProperty("label", "bucket");
      bucketDefinition.addProperty("value", bucketName);
      metric.add(bucketDefinition);
    } else if (mode == MetricMode.XDCR) {
      JsonObject source = new JsonObject();
      source.addProperty("label", "sourceBucketName");
      source.addProperty("value", bucketName);
      metric.add(source);
      JsonObject pipeLine = new JsonObject();
      pipeLine.addProperty("label", "pipelineType");
      pipeLine.addProperty("value", "Main");
      metric.add(pipeLine);
    }
    return metric;
  }

  public String generate(MetricValue metric, MetricType type, boolean decimal,
                         boolean perSecond, String transform, String label) {
    double finalValue;
    if (type == MetricType.COUNTER) {
      if (perSecond) {
        finalValue = metric.getPerSec();
      } else {
        finalValue = metric.getDelta();
      }
    } else {
      finalValue = metric.getValue();
    }

    switch (transform) {
      case "to_gib":
        return formatDataSize(finalValue, label);
      case "from_ns":
        return fromNanoSeconds(finalValue, label, decimal);
      case "inverse":
        return percentInv(finalValue, label, decimal);
      case "comma_delim":
        return commaDelimit(finalValue, label, decimal);
      case "to_ms":
        return toMilliSeconds(finalValue, label, decimal);
      case "percent":
        return percentage(finalValue, label, decimal);
      default:
        return defaultFormat(finalValue, label, decimal);
    }
  }

  private String defaultFormat(double value, String label, boolean decimal) {
    if (decimal) {
      return String.format("%s: %.2f ", label, value);
    } else {
      return String.format("%s: %d ", label, Math.round(value));
    }
  }

  private String toMilliSeconds(double seconds, String label, boolean decimal) {
    double value = seconds * 1000;
    if (decimal) {
      return String.format("%s: %.2f ms ", label, value);
    } else {
      return String.format("%s: %d ms ", label, Math.round(value));
    }
  }

  private String commaDelimit(double value, String label, boolean decimal) {
    DecimalFormat formatter;
    if (decimal) {
      formatter = new DecimalFormat("#,###.##");
    } else {
      formatter = new DecimalFormat("#,###");
    }
    return String.format("%s: %s ", label, formatter.format(value));
  }

  private String percentInv(double percentage, String label, boolean decimal) {
    double value = 100 - percentage;
    if (decimal) {
      return String.format("%s: %.2f %% ", label, value);
    } else {
      return String.format("%s: %d %% ", label, Math.round(value));
    }
  }

  private String percentage(double percentage, String label, boolean decimal) {
    if (decimal) {
      return String.format("%s: %.2f %% ", label, percentage);
    } else {
      return String.format("%s: %d %% ", label, Math.round(percentage));
    }
  }

  private String fromNanoSeconds(double nano, String label, boolean decimal) {
    double value = nano / 1000000;
    if (decimal) {
      return String.format("%s: %.2f ms ", label, value);
    } else {
      return String.format("%s: %d ms ", label, Math.round(value));
    }
  }

  private String formatDurationValue(double millis, String label, boolean decimal) {
    String output;

    double seconds = millis / 1000;
    double minutes = ((millis / 1000) / 60);
    double hours = (((millis / 1000) / 60) / 60);

    if (hours > 1) {
      output = decimal ? String.format("%.2f hr", hours) : String.format("%d hr", Math.round(hours));
    } else if (minutes > 1) {
      output = decimal ? String.format("%.2f min", minutes) : String.format("%d min", Math.round(minutes));
    } else if (seconds > 1) {
      output = decimal ? String.format("%.2f sec", seconds) : String.format("%d sec", Math.round(seconds));
    } else {
      output = decimal ? String.format("%.2f ms", millis) : String.format("%d ms", Math.round(millis));
    }

    return String.format("%s: %s ", label, output);
  }

  private String formatDataSize(double bytes, String label) {
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

  @Override
  public void stopCollectionThread() {
    System.err.println("Stopping remote statistics thread...");
    STATISTICS.info(String.format("==== End Cluster Collection %s ====\n", timeStampFormat.format(new Date())));
    apiHandle.cancel(true);
  }

  @Override
  public void getResults() {
  }
}
