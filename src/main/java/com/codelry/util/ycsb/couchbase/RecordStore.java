package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public interface RecordStore {
  void disconnect();

  Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result);

  Status insert(String table, String key, Map<String, ByteIterator> values);

  Status update(String table, String key, Map<String, ByteIterator> values);

  Status delete(String table, String key);

  Status scan(String table, String key, int count, Set<String> fields,
              Vector<HashMap<String, ByteIterator>> result);
}
