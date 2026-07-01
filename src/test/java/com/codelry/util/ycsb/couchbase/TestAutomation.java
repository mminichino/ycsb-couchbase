package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.Benchmark;
import com.codelry.util.ycsb.TestSetup;
import com.codelry.util.ycsb.TestSetupFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.codelry.util.ycsb.Benchmark.DO_TRANSACTIONS_PROPERTY;
import static com.codelry.util.ycsb.Benchmark.TEST_SETUP_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Invokes {@link CouchbaseTestSetup} the same way ycsb-core does during the load phase.
 */
class TestAutomation {

  private static final String DB_PROPERTY_FILE = "db.properties";

  @Test
  void couchbaseTestSetup() {
    Properties props = loadPropertiesForLoadPhase();

    String testSetupClass = props.getProperty(TEST_SETUP_PROPERTY);
    assertNotNull(testSetupClass, "test.setup property must be set");

    TestSetup testSetup = TestSetupFactory.newInstance(testSetupClass);
    assertNotNull(testSetup, "Failed to instantiate test setup class: " + testSetupClass);

    assertDoesNotThrow(() -> testSetup.testSetup(props));
  }

  /**
   * Loads properties the way {@link Benchmark#runLoadForWorkload(String)} does before calling
   * {@code runTestSetup}, including {@code db.properties} from the runtime classpath.
   */
  private static Properties loadPropertiesForLoadPhase() {
    return BenchmarkPropertiesLoader.load("a");
  }

  private static final class BenchmarkPropertiesLoader extends Benchmark {
    private BenchmarkPropertiesLoader() {
    }

    static Properties load(String workload) {
      Properties props = new Properties();
      props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
      new BenchmarkPropertiesLoader().loadPropertiesFiles(workload, props);
      loadPropertiesFromClasspath(props, DB_PROPERTY_FILE);
      return props;
    }
  }

  private static void loadPropertiesFromClasspath(Properties props, String propertyFile) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = TestAutomation.class.getClassLoader();
    }
    try (InputStream in = classLoader.getResourceAsStream(propertyFile)) {
      if (in == null) {
        throw new IllegalStateException("Properties resource not found on classpath: " + propertyFile);
      }
      props.load(in);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load properties file: " + propertyFile, e);
    }
  }
}
