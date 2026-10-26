package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.couchbase.lite.Collection;
import com.couchbase.lite.Database;
import com.couchbase.lite.DataSource;
import com.couchbase.lite.Document;
import com.couchbase.lite.Expression;
import com.couchbase.lite.Meta;
import com.couchbase.lite.MutableDocument;
import com.couchbase.lite.Ordering;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryBuilder;
import com.couchbase.lite.Result;
import com.couchbase.lite.ResultSet;
import com.couchbase.lite.SelectResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * JSON record store backed by a local Couchbase Lite collection.
 * Documents are stored as JSON objects with string field values, matching
 * {@link JsonRecordStore} semantics.
 *
 * <p>During YCSB load ({@code dotransactions=false}), inserts are buffered and
 * committed with {@link Database#inBatch} in batches of {@link #LOAD_BATCH_SIZE}.
 * During the transaction phase, each insert is saved atomically.
 */
public class LiteJsonRecordStore implements RecordStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(LiteJsonRecordStore.class);

  static final int LOAD_BATCH_SIZE = 100;

  private final Database database;
  private final Collection collection;
  private final boolean loadMode;
  private final List<MutableDocument> pendingInserts;

  public LiteJsonRecordStore(Database database, Collection collection, boolean loadMode) {
    this.database = database;
    this.collection = collection;
    this.loadMode = loadMode;
    this.pendingInserts = loadMode ? new ArrayList<>(LOAD_BATCH_SIZE) : List.of();
  }

  @Override
  public void disconnect() {
    flushPendingInserts();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Document doc = collection.getDocument(key);
      if (doc == null) {
        return Status.NOT_FOUND;
      }
      result.putAll(MapTranscoder.decodeMapRecord(doc.toJSON().getBytes(StandardCharsets.UTF_8)));
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
      Document doc = collection.getDocument(key);
      if (doc == null) {
        return Status.NOT_FOUND;
      }
      MutableDocument mutable = doc.toMutable();
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        mutable.setString(e.getKey(), e.getValue().toString());
      }
      collection.save(mutable);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("update transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      MutableDocument doc = new MutableDocument(key);
      doc.setJSON(new String(MapTranscoder.encodeMapRecord(values), StandardCharsets.UTF_8));
      if (loadMode) {
        return bufferInsert(doc);
      }
      collection.save(doc);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  private synchronized Status bufferInsert(MutableDocument doc) {
    pendingInserts.add(doc);
    if (pendingInserts.size() >= LOAD_BATCH_SIZE) {
      return flushPendingInsertsLocked();
    }
    return Status.OK;
  }

  private synchronized void flushPendingInserts() {
    if (!loadMode || pendingInserts.isEmpty()) {
      return;
    }
    Status status = flushPendingInsertsLocked();
    if (status != Status.OK) {
      LOGGER.error("Failed to flush pending Lite inserts on disconnect");
    }
  }

  private Status flushPendingInsertsLocked() {
    if (pendingInserts.isEmpty()) {
      return Status.OK;
    }
    List<MutableDocument> batch = new ArrayList<>(pendingInserts);
    pendingInserts.clear();
    try {
      database.inBatch(() -> {
        for (MutableDocument doc : batch) {
          collection.save(doc);
        }
      });
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("batch insert exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      Document doc = collection.getDocument(key);
      if (doc == null) {
        return Status.NOT_FOUND;
      }
      collection.delete(doc);
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
      Query query = QueryBuilder
          .select(SelectResult.expression(Meta.id).as("id"))
          .from(DataSource.collection(collection))
          .where(Meta.id.greaterThanOrEqualTo(Expression.string(startkey)))
          .orderBy(Ordering.expression(Meta.id).ascending())
          .limit(Expression.intValue(recordcount));

      try (ResultSet rs = query.execute()) {
        for (Result row : rs) {
          String docId = row.getString("id");
          Document doc = collection.getDocument(docId);
          if (doc != null) {
            result.add(MapTranscoder.decodeRecord(doc.toJSON().getBytes(StandardCharsets.UTF_8)));
          }
        }
      }
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }
}
