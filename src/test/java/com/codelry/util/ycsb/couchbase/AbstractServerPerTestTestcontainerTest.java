package com.codelry.util.ycsb.couchbase;

import com.codelry.util.cbdb3.CouchbaseConfig;
import com.codelry.util.cbdb3.CouchbaseConnect;
import com.codelry.util.cbdb3.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

/**
 * Base for tests that start a dedicated Couchbase container before each test
 * and stop it when the test finishes.
 */
abstract class AbstractServerPerTestTestcontainerTest extends AbstractServerTestcontainerTest {
  protected GenericContainer<?> couchbase;

  @BeforeEach
  void startContainerAndCluster() throws Exception {
    loadProperties();
    couchbase = CouchbaseServerContainer.startDedicatedContainer();
    initializeCluster();
  }

  @AfterEach
  void stopContainer() {
    Server.getInstance().disconnect();
    CouchbaseServerContainer.stopContainer(couchbase);
    couchbase = null;
  }

  private void initializeCluster() {
    String hostname = properties.getProperty(CouchbaseConfig.COUCHBASE_HOST, CouchbaseConfig.DEFAULT_HOSTNAME);
    CouchbaseConnect db = Server.getInstance();
    CouchbaseConfig config = serverConfig();
    Map<String, String> options = Map.of(
        String.format(CouchbaseConfig.COUCHBASE_SERVER_IP, 0), hostname,
        String.format(CouchbaseConfig.COUCHBASE_SERVER_RAM, 0), "4",
        String.format(CouchbaseConfig.COUCHBASE_SERVER_SERVICES, 0), "data,index,query,fts"
    );
    db.createCluster(config, options);
    Server.getInstance().disconnect();
  }
}
