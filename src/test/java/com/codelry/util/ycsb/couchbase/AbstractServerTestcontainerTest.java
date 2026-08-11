package com.codelry.util.ycsb.couchbase;

import com.codelry.util.cbdb3.CouchbaseConfig;
import com.codelry.util.cbdb3.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.Properties;

/**
 * Shared property loading and disconnect helpers for Couchbase Server Testcontainers tests.
 */
abstract class AbstractServerTestcontainerTest {
  protected static final Logger LOGGER = LogManager.getLogger(AbstractServerTestcontainerTest.class);
  protected static final String PROPERTY_FILE = "db.properties";
  protected static Properties properties;

  @BeforeAll
  static void loadProperties() throws IOException {
    if (properties != null) {
      return;
    }
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    properties = new Properties();
    LOGGER.info("Testing with properties file: {}", PROPERTY_FILE);
    properties.load(loader.getResourceAsStream(PROPERTY_FILE));
  }

  @AfterEach
  void disconnectServer() {
    Server.getInstance().disconnect();
  }

  protected static CouchbaseConfig serverConfig() {
    String hostname = properties.getProperty(CouchbaseConfig.COUCHBASE_HOST, CouchbaseConfig.DEFAULT_HOSTNAME);
    return new CouchbaseConfig()
        .fromProperties(properties)
        .host(hostname)
        .ssl(false);
  }
}
