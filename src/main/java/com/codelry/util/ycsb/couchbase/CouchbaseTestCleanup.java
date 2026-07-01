package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.TestCleanup;
import java.util.Properties;

import static com.codelry.util.ycsb.couchbase.RetryLogic.retryVoid;

import com.codelry.util.cbdb3.CouchbaseConnect;
import com.codelry.util.cbdb3.CouchbaseConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clean Cluster after Testing.
 */
public class CouchbaseTestCleanup extends TestCleanup {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTestCleanup.class);

  @Override
  public void testClean(Properties properties) {
    CouchbaseConnect db = CouchbaseConnect.getInstance();
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
    db.connect(config);

    try {
      LOGGER.info("Removing bucket {} on cluster:[{}]", db.getBucketName(), db.hostValue());
      retryVoid(db::dropBucket);
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
