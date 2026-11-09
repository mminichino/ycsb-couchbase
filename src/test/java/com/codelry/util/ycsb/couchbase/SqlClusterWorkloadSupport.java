package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.Benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.codelry.util.ycsb.Benchmark.DO_TRANSACTIONS_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared helpers for live-cluster SQL record store workload smoke tests.
 */
final class SqlClusterWorkloadSupport {
  private static final Pattern FAILED_OPS = Pattern.compile(
      "^\\[([A-Z0-9_-]+)],\\s*(?:Operations|Return=(?:NOT_FOUND|ERROR)),\\s*(\\d+)\\s*$");

  private SqlClusterWorkloadSupport() {
  }

  static void runWorkload(String workload, String host, String username, String password) {
    Benchmark benchmark = new Benchmark();
    benchmark.run(loadPhaseProperties(workload, host, username, password));
    benchmark.run(runPhaseProperties(workload, host, username, password));
    assertNoFailedOperations(workload);
  }

  static Properties loadPhaseProperties(String workload, String host, String username, String password) {
    Properties props = baseProperties(workload, host, username, password);
    props.setProperty(DO_TRANSACTIONS_PROPERTY, "false");
    props.setProperty("maxexecutiontime", "0");
    return props;
  }

  static Properties runPhaseProperties(String workload, String host, String username, String password) {
    Properties props = baseProperties(workload, host, username, password);
    props.setProperty(DO_TRANSACTIONS_PROPERTY, "true");
    return props;
  }

  static Properties baseProperties(String workload, String host, String username, String password) {
    Properties props = ClusterPropertiesLoader.load(workload);
    props.setProperty("couchbase.hostname", host);
    props.setProperty("couchbase.username", username);
    props.setProperty("couchbase.password", password);
    props.setProperty("couchbase.recordstore", "sql");
    props.setProperty("batchsize", Integer.toString(SQLRecordStore.DEFAULT_BATCH_SIZE));
    props.setProperty("recordcount", "200");
    props.setProperty("operationcount", "200");
    props.setProperty("insertcount", "200");
    props.setProperty("threadcount", "4");
    props.setProperty("maxexecutiontime", "60");
    return props;
  }

  static void assertNoFailedOperations(String workload) {
    String workloadFile = workloadFileName(workload);
    List<String> failures = new ArrayList<>();
    failures.addAll(collectFailures(Path.of("output", workloadFile + "-load.dat")));
    failures.addAll(collectFailures(Path.of("output", workloadFile + "-run.dat")));
    assertTrue(failures.isEmpty(),
        () -> "Expected zero failed operations for workload " + workload + ", found:\n"
            + String.join("\n", failures));
  }

  private static List<String> collectFailures(Path path) {
    List<String> failures = new ArrayList<>();
    if (!Files.isRegularFile(path)) {
      failures.add("missing output file: " + path);
      return failures;
    }
    try {
      for (String line : Files.readAllLines(path)) {
        Matcher matcher = FAILED_OPS.matcher(line.trim());
        if (!matcher.matches()) {
          continue;
        }
        String metric = matcher.group(1);
        long count = Long.parseLong(matcher.group(2));
        if (count <= 0) {
          continue;
        }
        boolean failedMetric = metric.endsWith("-FAILED")
            || line.contains("Return=NOT_FOUND")
            || line.contains("Return=ERROR");
        if (failedMetric) {
          failures.add(path + ": " + line);
        }
      }
    } catch (IOException e) {
      failures.add("unable to read " + path + ": " + e.getMessage());
    }
    return failures;
  }

  private static String workloadFileName(String workload) {
    return switch (workload.toLowerCase(Locale.ROOT)) {
      case "a" -> "workloada";
      case "b" -> "workloadb";
      case "c" -> "workloadc";
      case "d" -> "workloadd";
      case "e" -> "workloade";
      case "f" -> "workloadf";
      default -> workload.toLowerCase(Locale.ROOT);
    };
  }

  private static final class ClusterPropertiesLoader extends Benchmark {
    private ClusterPropertiesLoader() {
    }

    static Properties load(String workload) {
      Properties props = new Properties();
      new ClusterPropertiesLoader().loadPropertiesFiles(workload, props);
      loadPropertiesFromClasspath(props, "db.properties");
      return props;
    }
  }

  private static void loadPropertiesFromClasspath(Properties props, String resource) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = SqlClusterWorkloadSupport.class.getClassLoader();
    }
    try (InputStream in = classLoader.getResourceAsStream(resource)) {
      if (in != null) {
        props.load(in);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load properties file: " + resource, e);
    }
  }
}
