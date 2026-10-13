package com.codelry.util.ycsb.couchbase;

import com.codelry.util.cbdb3.CouchbaseConfig;
import com.codelry.util.cbdb3.CouchbaseConnect;
import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.DB;
import com.codelry.util.ycsb.DBException;
import com.codelry.util.ycsb.Status;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class that implements the Couchbase Java SDK to be used with YCSB.
 */
public class CouchbaseClientBinding extends DB {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseClientBinding.class);
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  public static final String COUCHBASE_RECORDSTORE = "couchbase.recordstore";
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Duration TTL = Duration.ofSeconds(0);
  private static volatile Cluster cluster;
  private static volatile Bucket bucket;
  private static volatile Collection collection;
  private static String query;
  private static volatile DurabilityLevel durability = DurabilityLevel.NONE;

  private RecordStore recordStore;

  @Override
  public void init() throws DBException {
    Properties properties = new Properties();
    properties.putAll(getProperties());

    String bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    String scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    String collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    String recordStoreType = properties.getProperty(COUCHBASE_RECORDSTORE, "json").toLowerCase();
    boolean debug = getProperties().getProperty("couchbase.debug", "false").equals("true");
    String keySpace = bucketName + "." + scopeName + "." + collectionName;
    query = "SELECT RAW META(t).id FROM " + keySpace + " AS t WHERE META(t).id >= $1 ORDER BY META(t).id LIMIT $2;";
    durability =
        setDurabilityLevel(Integer.parseInt(properties.getProperty("couchbase.durability", "0")));
    int ttlSeconds = Integer.parseInt(properties.getProperty("couchbase.ttlSeconds", "0"));
    TTL = Duration.ofSeconds(ttlSeconds);

    if (debug) {
      Configurator.setLevel(LOGGER.getName(), Level.DEBUG);
    }

    synchronized (INIT_COORDINATOR) {
      try {
        CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties)
            .kvTimeout(20)
            .connectTimeout(20)
            .queryTimeout(90);
        CouchbaseConnect db = CouchbaseConnect.getInstance();
        db.connect(config);

        cluster = db.getCluster();
        bucket = cluster.bucket(bucketName);
        collection = bucket.scope(scopeName).collection(collectionName);
      } catch (Exception e) {
        logError(e);
        throw new DBException(e);
      }
    }

    recordStore = createRecordStore(recordStoreType);
    OPEN_CLIENTS.incrementAndGet();
  }

  private RecordStore createRecordStore(String recordStoreType) throws DBException {
    return switch (recordStoreType) {
      case "json" -> new JsonRecordStore(cluster, collection, query, TTL, durability);
      case "protobuf" -> new ProtobufRecordStore(cluster, collection, query, TTL, durability);
      default -> throw new DBException("Unknown couchbase.recordstore value: " + recordStoreType
          + " (expected json or protobuf)");
    };
  }

  private void logError(Exception error) {
    LOGGER.error(error.getMessage(), error);
  }

  private DurabilityLevel setDurabilityLevel(final int value) {
    return switch (value) {
      case 1 -> DurabilityLevel.MAJORITY;
      case 2 -> DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
      case 3 -> DurabilityLevel.PERSIST_TO_MAJORITY;
      default -> DurabilityLevel.NONE;
    };
  }

  @Override
  public synchronized void cleanup() {
    if (recordStore != null) {
      recordStore.disconnect();
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
