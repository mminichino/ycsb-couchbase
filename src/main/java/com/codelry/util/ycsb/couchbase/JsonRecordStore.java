package com.codelry.util.ycsb.couchbase;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static java.lang.Math.min;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.UpsertOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * JSON record store: documents are stored as JSON objects with string field values.
 */
public class JsonRecordStore implements RecordStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonRecordStore.class);

  private final Cluster cluster;
  private final Collection collection;
  private final String query;
  private final UpsertOptions upsertOptions;
  private final MutateInOptions mutateInOptions;

  public JsonRecordStore(Cluster cluster, Collection collection, String query,
                         Duration ttl, DurabilityLevel durability) {
    this.cluster = cluster;
    this.collection = collection;
    this.query = query;
    this.upsertOptions = UpsertOptions.upsertOptions()
        .transcoder(RawJsonTranscoder.INSTANCE)
        .expiry(ttl)
        .durability(durability);
    this.mutateInOptions = MutateInOptions.mutateInOptions()
        .expiry(ttl)
        .durability(durability);
  }

  @Override
  public void disconnect() {
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      byte[] doc = collection.get(key).contentAsBytes();
      result.putAll(MapTranscoder.decodeMapRecord(doc));
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

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      List<MutateInSpec> specs = new ArrayList<>(values.size());
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        specs.add(MutateInSpec.upsert(e.getKey(), e.getValue().toString()));
      }
      collection.mutateIn(key, specs, mutateInOptions);
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      LOGGER.error("update transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      collection.upsert(key, MapTranscoder.encodeMapRecord(values), upsertOptions);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      collection.remove(key);
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      LOGGER.error("delete transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      List<byte[]> docs = cluster.reactive().query(query, queryOptions()
              .adhoc(false)
              .readonly(true)
              .metrics(false)
              .useReplica(true)
              .parameters(JsonArray.from(startkey, recordcount)))
          .flatMapMany(reactiveQueryResult -> reactiveQueryResult.rowsAs(String.class))
          .flatMapSequential(docId -> collection.reactive().get(docId)
              .map(GetResult::contentAsBytes)
              .onErrorResume(DocumentNotFoundException.class, e -> Mono.empty()), min(256, recordcount))
          .collectList()
          .block();
      assert docs != null;
      result.addAll(docs.stream().map(MapTranscoder::decodeRecord).toList());
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }
}
