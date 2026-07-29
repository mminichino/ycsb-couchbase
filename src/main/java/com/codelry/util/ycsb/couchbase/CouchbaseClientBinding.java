package com.codelry.util.ycsb.couchbase;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static java.lang.Math.min;

import com.codelry.util.cbdb3.CouchbaseConfig;
import com.codelry.util.ycsb.*;
import com.couchbase.client.core.env.*;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.json.JsonArray;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.java.kv.*;
import reactor.core.publisher.Mono;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.util.cbdb3.CouchbaseConnect;

/**
 * A class that implements the Couchbase Java SDK to be used with YCSB.
 */
public class CouchbaseClientBinding extends DB {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseClientBinding.class);
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Duration TTL = Duration.ofSeconds(0);
  private static final GetOptions GET_OPTIONS = GetOptions.getOptions().transcoder(MapTranscoder.INSTANCE);
  private static volatile MutateInOptions MUTATE_IN_OPTIONS = MutateInOptions.mutateInOptions()
      .expiry(TTL)
      .durability(DurabilityLevel.NONE);
  private static volatile UpsertOptions UPSERT_OPTIONS = UpsertOptions.upsertOptions()
      .expiry(TTL)
      .durability(DurabilityLevel.NONE)
      .transcoder(MapTranscoder.INSTANCE);
  private static volatile Cluster cluster;
  private static volatile Bucket bucket;
  private static volatile Collection collection;
  private static String query;
  private static volatile DurabilityLevel durability = DurabilityLevel.NONE;

  @Override
  public void init() throws DBException {
    Properties properties = new Properties();
    properties.putAll(getProperties());

    String bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    String scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    String collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    boolean debug = getProperties().getProperty("couchbase.debug", "false").equals("true");
    String keySpace = bucketName + "." + scopeName + "." + collectionName;
    query = "SELECT RAW META(t).id FROM " + keySpace + " AS t WHERE META(t).id >= $1 ORDER BY META(t).id LIMIT $2;";
    durability =
        setDurabilityLevel(Integer.parseInt(properties.getProperty("couchbase.durability", "0")));
    int ttlSeconds = Integer.parseInt(properties.getProperty("couchbase.ttlSeconds", "0"));

    if (ttlSeconds > 0 || durability != DurabilityLevel.NONE) {
      TTL = Duration.ofSeconds(ttlSeconds);
      UPSERT_OPTIONS = UpsertOptions.upsertOptions()
          .expiry(TTL)
          .durability(durability)
          .transcoder(MapTranscoder.INSTANCE);
      MUTATE_IN_OPTIONS = MutateInOptions.mutateInOptions()
          .expiry(TTL)
          .durability(durability);
    }

    if (debug) {
      Configurator.setLevel(LOGGER.getName(), Level.DEBUG);
    }

    synchronized (INIT_COORDINATOR) {
      try {
        CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties)
            .kvTimeout(10)
            .connectTimeout(20)
            .queryTimeout(90);
        CouchbaseConnect db = CouchbaseConnect.getInstance();
        db.connect(config);

        cluster = db.getCluster();
        bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(5));
        collection = bucket.scope(scopeName).collection(collectionName);
      } catch(Exception e) {
        logError(e);
        throw new DBException(e);
      }
    }

    OPEN_CLIENTS.incrementAndGet();
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
  }

  /**
   * Perform key/value read ("get").
   * @param table The name of the table.
   * @param key The record key of the record to read.
   * @param fields The list of fields to read or null for all of them.
   * @param result A Map of field/value pairs for the result.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      @SuppressWarnings("unchecked")
      Map<String, ByteIterator> doc = collection.get(key, GET_OPTIONS).contentAs(Map.class);
      result.putAll(doc);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(result.toString());
      }
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      LOGGER.error("read transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
   * Update record.
   * @param table The name of the table.
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record.
   */
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      List<MutateInSpec> specs = new ArrayList<>(values.size());
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        specs.add(MutateInSpec.upsert(e.getKey(), e.getValue().toString()));
      }
      collection.mutateIn(key, specs, MUTATE_IN_OPTIONS);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("update transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
   * Insert a record.
   * @param table The name of the table.
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record.
   */
  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      collection.upsert(key, values, UPSERT_OPTIONS);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("update transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
   * Remove a record.
   * @param table The name of the table.
   * @param key The record key of the record to delete.
   */
  @Override
  public Status delete(final String table, final String key) {
    try {
      collection.remove(key);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("delete transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
   * Query for specific rows of data using SQL++.
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param fields The list of fields to read or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set of field/value pairs for one record.
   */
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      cluster.reactive().query(query, queryOptions()
              .adhoc(false)
              .readonly(true)
              .metrics(false)
              .parameters(JsonArray.from(startkey, recordcount)))
          .flatMapMany(reactiveQueryResult -> reactiveQueryResult.rowsAs(String.class))
          .flatMapSequential(docId -> collection.reactive().get(docId, GET_OPTIONS)
              .map(getResult -> {
                @SuppressWarnings("unchecked")
                HashMap<String, ByteIterator> record = getResult.contentAs(HashMap.class);
                return record;
              })
              .onErrorResume(DocumentNotFoundException.class, e -> Mono.empty()), min(256, recordcount))
          .doOnNext(result::add)
          .then()
          .block();
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }
}
