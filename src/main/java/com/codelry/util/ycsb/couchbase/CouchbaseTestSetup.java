package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.TestSetup;

import java.util.List;
import java.util.Properties;

import static com.codelry.util.ycsb.couchbase.RetryLogic.retryVoid;

import com.codelry.util.cbdb3.CouchbaseConnect;
import com.codelry.util.cbdb3.CouchbaseConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepare Cluster for Testing.
 */
public class CouchbaseTestSetup extends TestSetup {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTestSetup.class);

  @Override
  public void testSetup(Properties properties) {
    String indexName = "idx_meta_id";

    CouchbaseConnect db = CouchbaseConnect.getInstance();
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
    db.connect(config);

    try {
      LOGGER.info("Creating bucket {} ({}) on cluster:[{}]", db.getBucketName(), config.getBucketStorage().toString(), db.hostValue());
      retryVoid(db::createBucket);
      LOGGER.info("Creating scope {}", db.getScopeName());
      retryVoid(db::createScope);
      LOGGER.info("Creating collection {}", db.getCollectionName());
      retryVoid(db::createCollection);
      LOGGER.info("Creating index {} on {}", indexName, db.getCollectionName());
      db.createSecondaryIndex(indexName, List.of("META().id"));
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
