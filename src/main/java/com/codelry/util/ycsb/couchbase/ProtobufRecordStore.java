package com.codelry.util.ycsb.couchbase;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static java.lang.Math.min;

import com.codelry.util.ycsb.ByteArrayByteIterator;
import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Protobuf record store: documents are opaque binary YcsbRecord protobuf messages.
 */
public class ProtobufRecordStore implements RecordStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufRecordStore.class);

  private final Cluster cluster;
  private final Collection collection;
  private final String query;
  private final UpsertOptions upsertOptions;
  private final ReplaceOptions replaceOptions;

  public ProtobufRecordStore(Cluster cluster, Collection collection, String query,
                             Duration ttl, DurabilityLevel durability) {
    this.cluster = cluster;
    this.collection = collection;
    this.query = query;
    this.upsertOptions = UpsertOptions.upsertOptions()
        .transcoder(RawBinaryTranscoder.INSTANCE)
        .expiry(ttl)
        .durability(durability);
    this.replaceOptions = ReplaceOptions.replaceOptions()
        .transcoder(RawBinaryTranscoder.INSTANCE)
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
      result.putAll(decodeRecord(doc));
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
      byte[] existing = collection.get(key).contentAsBytes();
      Map<String, ByteIterator> merged = decodeRecord(existing);
      merged.putAll(toByteIteratorMap(values));
      collection.replace(key, encodeRecord(merged), replaceOptions);
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
      collection.upsert(key, encodeRecord(values), upsertOptions);
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
      for (byte[] doc : docs) {
        result.add(decodeRecord(doc));
      }
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  static byte[] encodeRecord(Map<String, ByteIterator> values) {
    YcsbRecord.Builder builder = YcsbRecord.newBuilder();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      builder.putFields(entry.getKey(), ByteString.copyFrom(entry.getValue().toArray()));
    }
    return builder.build().toByteArray();
  }

  static HashMap<String, ByteIterator> decodeRecord(byte[] bytes) throws Exception {
    YcsbRecord record = YcsbRecord.parseFrom(bytes);
    HashMap<String, ByteIterator> result = new HashMap<>(record.getFieldsCount());
    for (Map.Entry<String, ByteString> entry : record.getFieldsMap().entrySet()) {
      result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().toByteArray()));
    }
    return result;
  }

  /**
   * Materialize update values once so iterators can be safely merged into the existing record.
   */
  private static Map<String, ByteIterator> toByteIteratorMap(Map<String, ByteIterator> values) {
    Map<String, ByteIterator> copy = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      copy.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().toArray()));
    }
    return copy;
  }
}
