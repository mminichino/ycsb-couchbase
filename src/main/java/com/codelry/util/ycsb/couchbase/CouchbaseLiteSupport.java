package com.codelry.util.ycsb.couchbase;

import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

/**
 * Shared helpers for local Couchbase Lite database configuration and lifecycle.
 */
final class CouchbaseLiteSupport {
  static final String COUCHBASE_BUCKET = "couchbase.bucket";
  static final String COUCHBASE_SCOPE = "couchbase.scope";
  static final String COUCHBASE_COLLECTION = "couchbase.collection";
  static final String COUCHBASE_LITE_DIRECTORY = "couchbase.lite.directory";
  static final String DEFAULT_DIRECTORY = ".couchbase/ycsb";

  private static final Object INIT_LOCK = new Object();
  private static volatile boolean initialized;

  private CouchbaseLiteSupport() {
  }

  static void ensureInitialized() {
    if (!initialized) {
      synchronized (INIT_LOCK) {
        if (!initialized) {
          CouchbaseLite.init();
          initialized = true;
        }
      }
    }
  }

  static String databaseName(Properties properties) {
    return properties.getProperty(COUCHBASE_BUCKET, "ycsb");
  }

  static String scopeName(Properties properties) {
    return properties.getProperty(COUCHBASE_SCOPE, "_default");
  }

  static String collectionName(Properties properties) {
    return properties.getProperty(COUCHBASE_COLLECTION, "_default");
  }

  static File directory(Properties properties) {
    String path = properties.getProperty(COUCHBASE_LITE_DIRECTORY, DEFAULT_DIRECTORY);
    return new File(path);
  }

  static File ensureDirectory(Properties properties) throws IOException {
    File directory = directory(properties);
    if (!directory.exists()) {
      Files.createDirectories(directory.toPath());
    }
    return directory;
  }

  static DatabaseConfiguration databaseConfiguration(File directory) {
    DatabaseConfiguration config = new DatabaseConfiguration();
    config.setDirectory(directory.getAbsolutePath());
    return config;
  }

  static Database openDatabase(Properties properties) throws CouchbaseLiteException, IOException {
    ensureInitialized();
    File directory = ensureDirectory(properties);
    return new Database(databaseName(properties), databaseConfiguration(directory));
  }
}
