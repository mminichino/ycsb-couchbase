package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.TestCleanup;
import com.couchbase.lite.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * Remove the local Couchbase Lite database after testing.
 * If no database file exists, cleanup completes without error.
 */
public class CouchbaseLiteTestCleanup extends TestCleanup {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseLiteTestCleanup.class);

  @Override
  public void testClean(Properties properties) {
    try {
      CouchbaseLiteSupport.ensureInitialized();

      String dbName = CouchbaseLiteSupport.databaseName(properties);
      File directory = CouchbaseLiteSupport.directory(properties);

      if (!Database.exists(dbName, directory)) {
        LOGGER.info("Lite database {} not found in {}; nothing to remove",
            dbName, directory.getAbsolutePath());
        return;
      }

      LOGGER.info("Removing Lite database {} in {}", dbName, directory.getAbsolutePath());
      Database.delete(dbName, directory);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
