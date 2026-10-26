package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.Benchmark;
import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CouchbaseLiteBindingSmokeTest {

  @TempDir
  Path tempDir;

  private Properties properties;
  private CouchbaseLiteClientBinding db;

  @BeforeEach
  void setUp() throws Exception {
    properties = new Properties();
    properties.setProperty("couchbase.bucket", "bench");
    properties.setProperty("couchbase.scope", "bench");
    properties.setProperty("couchbase.collection", "usertable");
    properties.setProperty("couchbase.lite.directory", tempDir.toString());
    // Default to transaction mode so inserts are visible immediately.
    properties.setProperty(Benchmark.DO_TRANSACTIONS_PROPERTY, "true");

    new CouchbaseLiteTestSetup().testSetup(properties);

    db = new CouchbaseLiteClientBinding();
    db.setProperties(properties);
    db.init();
  }

  @AfterEach
  void tearDown() {
    if (db != null) {
      db.cleanup();
      db = null;
    }
    assertDoesNotThrow(() -> new CouchbaseLiteTestCleanup().testClean(properties));
    assertDoesNotThrow(() -> new CouchbaseLiteTestCleanup().testClean(properties));
  }

  @Test
  void crudAndScan() {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new StringByteIterator("hello"));
    values.put("field1", new StringByteIterator("world"));
    assertEquals(Status.OK, db.insert("usertable", "user1", values));

    Map<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, db.read("usertable", "user1", null, result));
    assertEquals("hello", result.get("field0").toString());
    assertEquals("world", result.get("field1").toString());

    Map<String, ByteIterator> update = new HashMap<>();
    update.put("field0", new StringByteIterator("updated"));
    assertEquals(Status.OK, db.update("usertable", "user1", update));

    result.clear();
    assertEquals(Status.OK, db.read("usertable", "user1", null, result));
    assertEquals("updated", result.get("field0").toString());
    assertEquals("world", result.get("field1").toString());

    Vector<HashMap<String, ByteIterator>> scan = new Vector<>();
    assertEquals(Status.OK, db.scan("usertable", "user1", 10, null, scan));
    assertEquals(1, scan.size());

    assertEquals(Status.OK, db.delete("usertable", "user1"));
    assertEquals(Status.NOT_FOUND, db.read("usertable", "user1", null, new HashMap<>()));
  }

  @Test
  void loadModeBatchesInsertsAndFlushesRemainder() throws Exception {
    db.cleanup();
    db = null;

    properties.setProperty(Benchmark.DO_TRANSACTIONS_PROPERTY, "false");
    db = new CouchbaseLiteClientBinding();
    db.setProperties(properties);
    db.init();

    int total = LiteJsonRecordStore.LOAD_BATCH_SIZE + 50;
    for (int i = 0; i < total; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("field0", new StringByteIterator("v" + i));
      assertEquals(Status.OK, db.insert("usertable", "user" + i, values));
    }

    // First full batch should already be committed; remainder is still buffered.
    assertEquals(Status.OK, db.read("usertable", "user0", null, new HashMap<>()));
    assertEquals(Status.NOT_FOUND, db.read("usertable", "user" + (total - 1), null, new HashMap<>()));

    db.cleanup();
    db = null;

    // Reopen in transaction mode; leftover batch should have been flushed on cleanup.
    properties.setProperty(Benchmark.DO_TRANSACTIONS_PROPERTY, "true");
    db = new CouchbaseLiteClientBinding();
    db.setProperties(properties);
    db.init();

    Map<String, ByteIterator> first = new HashMap<>();
    Map<String, ByteIterator> last = new HashMap<>();
    assertEquals(Status.OK, db.read("usertable", "user0", null, first));
    assertEquals(Status.OK, db.read("usertable", "user" + (total - 1), null, last));
    assertEquals("v0", first.get("field0").toString());
    assertEquals("v" + (total - 1), last.get("field0").toString());
    assertNotEquals(Status.NOT_FOUND, db.read("usertable", "user" + LiteJsonRecordStore.LOAD_BATCH_SIZE, null,
        new HashMap<>()));
  }
}
