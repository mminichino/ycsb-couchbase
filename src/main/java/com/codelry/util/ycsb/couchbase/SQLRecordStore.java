package com.codelry.util.ycsb.couchbase;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * JSON record store that performs all YCSB operations via SQL++ (no KV API).
 * Point ops use {@code USE KEYS}; range scan uses {@code META().id} against the setup index.
 * Documents use the same {@link MapTranscoder} serialization as {@link JsonRecordStore}.
 *
 * <p>Inserts are buffered and flushed in SQL++ multi-document {@code UPSERT} batches of
 * {@link #DEFAULT_BATCH_SIZE} (configurable via {@code batchsize}), matching the YCSB postgres
 * relational store load pattern.
 */
public class SQLRecordStore implements RecordStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLRecordStore.class);

  public static final int DEFAULT_BATCH_SIZE = 100;

  private final Cluster cluster;
  private final Duration ttl;
  private final DurabilityLevel durability;
  private final int batchSize;
  private final List<PendingInsert> pendingInserts;
  private final String readQuery;
  private final String scanQuery;
  private final String upsertQuery;
  private final String batchUpsertQuery;
  private final String updateQuery;
  private final String deleteQuery;

  public SQLRecordStore(Cluster cluster, String keySpace, Duration ttl, DurabilityLevel durability) {
    this(cluster, keySpace, ttl, durability, DEFAULT_BATCH_SIZE);
  }

  public SQLRecordStore(Cluster cluster, String keySpace, Duration ttl, DurabilityLevel durability,
                        int batchSize) {
    this.cluster = cluster;
    this.ttl = ttl;
    this.durability = durability;
    this.batchSize = Math.max(1, batchSize);
    this.pendingInserts = new ArrayList<>(this.batchSize);
    this.readQuery = "SELECT RAW t FROM " + keySpace + " AS t USE KEYS [$1]";
    this.scanQuery = "SELECT RAW t FROM " + keySpace
        + " AS t WHERE META(t).id >= $1 ORDER BY META(t).id LIMIT $2";
    this.deleteQuery = "DELETE FROM " + keySpace + " AS t USE KEYS [$1]";
    if (ttl.isZero()) {
      this.upsertQuery = "UPSERT INTO " + keySpace + " (KEY, VALUE) VALUES ($1, $2)";
      this.batchUpsertQuery = "UPSERT INTO " + keySpace
          + " (KEY k, VALUE v) SELECT d.k AS k, d.v AS v FROM $docs AS d";
      // UPSERT overwrite avoids CAS mismatch under concurrent updates (last write wins).
      this.updateQuery = "UPSERT INTO " + keySpace
          + " (KEY k, VALUE v) SELECT META(t).id AS k, OBJECT_CONCAT(t, $2) AS v"
          + " FROM " + keySpace + " AS t USE KEYS [$1]";
    } else {
      this.upsertQuery = "UPSERT INTO " + keySpace + " (KEY, VALUE, OPTIONS) VALUES ($1, $2, $3)";
      this.batchUpsertQuery = "UPSERT INTO " + keySpace
          + " (KEY k, VALUE v, OPTIONS o) SELECT d.k AS k, d.v AS v, {\"expiration\": "
          + ttl.getSeconds() + "} AS o FROM $docs AS d";
      this.updateQuery = "UPSERT INTO " + keySpace
          + " (KEY k, VALUE v, OPTIONS o) SELECT META(t).id AS k, OBJECT_CONCAT(t, $2) AS v, $3 AS o"
          + " FROM " + keySpace + " AS t USE KEYS [$1]";
    }
  }

  @Override
  public void disconnect() {
    Status status = flushPendingInserts();
    if (status != Status.OK && status != Status.BATCHED_OK) {
      LOGGER.error("Failed to flush pending SQL inserts on disconnect: {}", status);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Status flushed = flushPendingInserts();
    if (!flushed.isOk()) {
      return flushed;
    }
    try {
      QueryResult queryResult = cluster.query(readQuery, pointReadOptions(JsonArray.from(key)));
      List<JsonObject> rows = queryResult.rowsAs(JsonObject.class);
      if (rows.isEmpty()) {
        return Status.NOT_FOUND;
      }
      result.putAll(MapTranscoder.decodeMapRecord(rows.get(0).toBytes()));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(result.toString());
      }
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("read transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Status flushed = flushPendingInserts();
    if (!flushed.isOk()) {
      return flushed;
    }
    try {
      JsonObject patch = JsonObject.fromJson(MapTranscoder.encodeMapRecord(values));
      JsonArray params = ttl.isZero()
          ? JsonArray.from(key, patch)
          : JsonArray.from(key, patch, JsonObject.create().put("expiration", ttl.getSeconds()));
      QueryResult queryResult = cluster.query(updateQuery, mutationOptions(params));
      if (mutationCount(queryResult) == 0) {
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("update transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (batchSize <= 1) {
      return executeInsert(key, values);
    }

    JsonObject doc = JsonObject.fromJson(MapTranscoder.encodeMapRecord(values));
    synchronized (pendingInserts) {
      pendingInserts.add(new PendingInsert(key, doc));
      if (pendingInserts.size() < batchSize) {
        return Status.BATCHED_OK;
      }
    }
    return flushPendingInserts();
  }

  @Override
  public Status delete(String table, String key) {
    Status flushed = flushPendingInserts();
    if (!flushed.isOk()) {
      return flushed;
    }
    try {
      QueryResult queryResult = cluster.query(deleteQuery, mutationOptions(JsonArray.from(key)));
      if (mutationCount(queryResult) == 0) {
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("delete transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    Status flushed = flushPendingInserts();
    if (!flushed.isOk()) {
      return flushed;
    }
    int attempts = 0;
    while (true) {
      try {
        QueryResult queryResult = cluster.query(scanQuery, scanOptions(JsonArray.from(startkey, recordcount)));
        List<JsonObject> rows = queryResult.rowsAs(JsonObject.class);
        result.addAll(rows.stream()
            .map(row -> MapTranscoder.decodeRecord(row.toBytes()))
            .toList());
        return Status.OK;
      } catch (Throwable t) {
        attempts++;
        if (attempts >= 5 || !isTransientIndexError(t)) {
          LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
          return Status.ERROR;
        }
        try {
          Thread.sleep(200L * attempts);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOGGER.error("scan retry interrupted: {}", t.getMessage(), t);
          return Status.ERROR;
        }
      }
    }
  }

  private Status executeInsert(String key, Map<String, ByteIterator> values) {
    try {
      JsonObject doc = JsonObject.fromJson(MapTranscoder.encodeMapRecord(values));
      JsonArray params;
      if (ttl.isZero()) {
        params = JsonArray.from(key, doc);
      } else {
        params = JsonArray.from(key, doc, JsonObject.create().put("expiration", ttl.getSeconds()));
      }
      cluster.query(upsertQuery, mutationOptions(params));
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  private Status flushPendingInserts() {
    List<PendingInsert> batch;
    synchronized (pendingInserts) {
      if (pendingInserts.isEmpty()) {
        return Status.OK;
      }
      batch = new ArrayList<>(pendingInserts);
      pendingInserts.clear();
    }

    try {
      JsonArray docs = JsonArray.create();
      for (PendingInsert insert : batch) {
        docs.add(JsonObject.create().put("k", insert.key).put("v", insert.doc));
      }
      cluster.query(batchUpsertQuery, mutationOptions(JsonObject.create().put("docs", docs)));
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("batched insert exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  private QueryOptions pointReadOptions(JsonArray params) {
    return queryOptions()
        .adhoc(false)
        .readonly(true)
        .metrics(false)
        .parameters(params);
  }

  private QueryOptions scanOptions(JsonArray params) {
    // Range scan uses idx_meta_id; REQUEST_PLUS avoids empty scans right after load UPSERTs.
    return queryOptions()
        .adhoc(false)
        .readonly(true)
        .metrics(false)
        .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
        .parameters(params);
  }

  private static boolean isTransientIndexError(Throwable t) {
    String message = String.valueOf(t.getMessage());
    return message.contains("Index not found") || message.contains("indexing.error");
  }

  private QueryOptions mutationOptions(JsonArray params) {
    return applyMutationOptions(queryOptions().parameters(params));
  }

  private QueryOptions mutationOptions(JsonObject params) {
    return applyMutationOptions(queryOptions().parameters(params));
  }

  private QueryOptions applyMutationOptions(QueryOptions options) {
    options.adhoc(false).metrics(true);
    if (durability != DurabilityLevel.NONE) {
      options.raw("durability_level", durabilityLevelParam(durability));
    }
    return options;
  }

  private static long mutationCount(QueryResult queryResult) {
    return queryResult.metaData().metrics()
        .map(metrics -> metrics.mutationCount())
        .orElse(0L);
  }

  private static String durabilityLevelParam(DurabilityLevel durability) {
    return switch (durability) {
      case MAJORITY -> "majority";
      case MAJORITY_AND_PERSIST_TO_ACTIVE -> "majorityAndPersistActive";
      case PERSIST_TO_MAJORITY -> "persistToMajority";
      default -> "none";
    };
  }

  private record PendingInsert(String key, JsonObject doc) {
  }
}
