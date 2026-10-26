package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.Benchmark;
import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.DB;
import com.codelry.util.ycsb.DBException;
import com.codelry.util.ycsb.Status;
import com.couchbase.lite.Collection;
import com.couchbase.lite.Database;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding that stores JSON documents in a local Couchbase Lite database.
 */
public class CouchbaseLiteClientBinding extends DB {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseLiteClientBinding.class);

  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Database database;
  private static volatile Collection collection;

  private RecordStore recordStore;

  @Override
  public void init() throws DBException {
    Properties properties = new Properties();
    properties.putAll(getProperties());

    boolean debug = properties.getProperty("couchbase.debug", "false").equals("true");
    if (debug) {
      Configurator.setLevel(LOGGER.getName(), Level.DEBUG);
    }

    // YCSB load sets dotransactions=false; run/transactions defaults to true.
    boolean loadMode = !Boolean.parseBoolean(
        properties.getProperty(Benchmark.DO_TRANSACTIONS_PROPERTY, "true"));

    String scopeName = CouchbaseLiteSupport.scopeName(properties);
    String collectionName = CouchbaseLiteSupport.collectionName(properties);

    synchronized (INIT_COORDINATOR) {
      try {
        if (database == null) {
          database = CouchbaseLiteSupport.openDatabase(properties);
          collection = database.getCollection(collectionName, scopeName);
          if (collection == null) {
            throw new DBException("Lite collection not found: " + scopeName + "." + collectionName
                + " (run CouchbaseLiteTestSetup first)");
          }
          LOGGER.info("Opened Couchbase Lite database {} at {} (loadMode={}, insertBatchSize={})",
              database.getName(), database.getPath(), loadMode,
              loadMode ? LiteJsonRecordStore.LOAD_BATCH_SIZE : 1);
        }
      } catch (DBException e) {
        throw e;
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        throw new DBException(e);
      }
    }

    recordStore = new LiteJsonRecordStore(database, collection, loadMode);
    OPEN_CLIENTS.incrementAndGet();
  }

  @Override
  public synchronized void cleanup() {
    if (recordStore != null) {
      recordStore.disconnect();
    }
    if (OPEN_CLIENTS.decrementAndGet() <= 0) {
      synchronized (INIT_COORDINATOR) {
        if (database != null) {
          try {
            database.close();
          } catch (Exception e) {
            LOGGER.warn("Error closing Couchbase Lite database: {}", e.getMessage(), e);
          } finally {
            database = null;
            collection = null;
          }
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return recordStore.read(table, key, fields, result);
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    return recordStore.update(table, key, values);
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    return recordStore.insert(table, key, values);
  }

  @Override
  public Status delete(final String table, final String key) {
    return recordStore.delete(table, key);
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return recordStore.scan(table, startkey, recordcount, fields, result);
  }
}
