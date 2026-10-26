package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.TestSetup;
import com.couchbase.lite.Collection;
import com.couchbase.lite.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * Create and prepare an empty local Couchbase Lite database for testing.
 * Deletes any existing database file first, then creates the configured
 * scope and collection.
 */
public class CouchbaseLiteTestSetup extends TestSetup {
  static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseLiteTestSetup.class);

  @Override
  public void testSetup(Properties properties) {
    try {
      CouchbaseLiteSupport.ensureInitialized();

      String dbName = CouchbaseLiteSupport.databaseName(properties);
      File directory = CouchbaseLiteSupport.ensureDirectory(properties);
      String scopeName = CouchbaseLiteSupport.scopeName(properties);
      String collectionName = CouchbaseLiteSupport.collectionName(properties);

      if (Database.exists(dbName, directory)) {
        LOGGER.info("Removing existing Lite database {} in {}", dbName, directory.getAbsolutePath());
        Database.delete(dbName, directory);
      }

      LOGGER.info("Creating Lite database {} in {}", dbName, directory.getAbsolutePath());
      Database database = new Database(dbName, CouchbaseLiteSupport.databaseConfiguration(directory));
      try {
        LOGGER.info("Creating collection {}.{}", scopeName, collectionName);
        Collection collection = database.createCollection(collectionName, scopeName);
        LOGGER.info("Prepared empty Lite collection {}", collection.getFullName());
      } finally {
        database.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
