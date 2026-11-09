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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * JSON record store that performs all YCSB operations via SQL++ queries keyed on {@code META().id}
 * (no KV API). Documents use the same {@link MapTranscoder} serialization as {@link JsonRecordStore}.
 */
public class SQLRecordStore implements RecordStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLRecordStore.class);

  private final Cluster cluster;
  private final Duration ttl;
  private final DurabilityLevel durability;
  private final String readQuery;
  private final String scanQuery;
  private final String upsertQuery;
  private final String updateQuery;
  private final String deleteQuery;

  public SQLRecordStore(Cluster cluster, String keySpace, Duration ttl, DurabilityLevel durability) {
    this.cluster = cluster;
    this.ttl = ttl;
    this.durability = durability;
    this.readQuery = "SELECT RAW t FROM " + keySpace + " AS t WHERE META(t).id = $1";
    this.scanQuery = "SELECT RAW t FROM " + keySpace
        + " AS t WHERE META(t).id >= $1 ORDER BY META(t).id LIMIT $2";
    this.deleteQuery = "DELETE FROM " + keySpace + " AS t WHERE META(t).id = $1";
    if (ttl.isZero()) {
      this.upsertQuery = "UPSERT INTO " + keySpace + " (KEY, VALUE) VALUES ($1, $2)";
      this.updateQuery = "UPDATE " + keySpace
          + " AS t SET t = OBJECT_CONCAT(t, $2) WHERE META(t).id = $1";
    } else {
      this.upsertQuery = "UPSERT INTO " + keySpace + " (KEY, VALUE, OPTIONS) VALUES ($1, $2, $3)";
      this.updateQuery = "UPDATE " + keySpace
          + " AS t SET t = OBJECT_CONCAT(t, $2), META(t).expiration = $3 WHERE META(t).id = $1";
    }
  }

  @Override
  public void disconnect() {
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      QueryResult queryResult = cluster.query(readQuery, readOptions(JsonArray.from(key)));
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
    try {
      JsonObject patch = JsonObject.fromJson(MapTranscoder.encodeMapRecord(values));
      JsonArray params = ttl.isZero()
          ? JsonArray.from(key, patch)
          : JsonArray.from(key, patch, ttl.getSeconds());
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

  @Override
  public Status delete(String table, String key) {
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
    try {
      QueryResult queryResult = cluster.query(scanQuery, readOptions(JsonArray.from(startkey, recordcount)));
      List<JsonObject> rows = queryResult.rowsAs(JsonObject.class);
      result.addAll(rows.stream()
          .map(row -> MapTranscoder.decodeRecord(row.toBytes()))
          .toList());
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  private QueryOptions readOptions(JsonArray params) {
    return queryOptions()
        .adhoc(false)
        .readonly(true)
        .metrics(false)
        .useReplica(true)
        .parameters(params);
  }

  private QueryOptions mutationOptions(JsonArray params) {
    QueryOptions options = queryOptions()
        .adhoc(false)
        .metrics(true)
        .parameters(params);
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
}
