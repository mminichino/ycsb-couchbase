/*
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.couchbase3;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.MutateInSpec.arrayAppend;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

/**
 * A class that implements the Couchbase Java SDK to be used with YCSB.
 */
public class CouchbaseQuery extends DB {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseQuery");
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  public static final String COUCHBASE_HOST = "couchbase.hostname";
  public static final String COUCHBASE_USER = "couchbase.username";
  public static final String COUCHBASE_PASSWORD = "couchbase.password";
  public static final String COUCHBASE_SSL_MODE = "couchbase.sslMode";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Cluster cluster;
  private static volatile Bucket bucket;
  private static volatile Collection collection;
  private static volatile ClusterEnvironment environment;
  private static String bucketName;
  private static String scopeName;
  private static String collectionName;
  private boolean adhoc;
  private int maxParallelism;
  private static int ttlSeconds;
  private boolean arrayMode;
  private String arrayKey;
  private Transcoder transcoder;
  private final MapSerializer serializer = new MapSerializer();
  private Class<?> contentType;
  private static volatile DurabilityLevel durability = DurabilityLevel.NONE;
  private static final AtomicLong recordNumber = new AtomicLong(0);

  @Override
  public void init() throws DBException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();
    String couchbasePrefix;

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        properties.load(propFile.openStream());
      } catch (IOException e) {
        throw new DBException(e);
      }
    }

    properties.putAll(getProperties());

    String hostname = properties.getProperty(COUCHBASE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String username = properties.getProperty(COUCHBASE_USER, CouchbaseConnect.DEFAULT_USER);
    String password = properties.getProperty(COUCHBASE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    boolean sslMode = properties.getProperty(COUCHBASE_SSL_MODE, "false").equals("true");
    boolean debug = getProperties().getProperty("couchbase.debug", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    boolean nativeCodec = getProperties().getProperty("couchbase.codec", "ycsb").equals("native");

    if (nativeCodec) {
      transcoder = RawJsonTranscoder.INSTANCE;
      contentType = String.class;
    } else {
      transcoder = MapTranscoder.INSTANCE;
      contentType = Map.class;
    }

    durability =
        setDurabilityLevel(Integer.parseInt(properties.getProperty("couchbase.durability", "0")));

    arrayMode = properties.getProperty("couchbase.mode", "default").equals("array");
    arrayKey = properties.getProperty("subdoc.arrayKey", "DataArray");

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));
    int kvEndpoints = Integer.parseInt(properties.getProperty("couchbase.kvEndpoints", "4"));

    long kvTimeout = Long.parseLong(properties.getProperty("couchbase.kvTimeout", "5"));
    long connectTimeout = Long.parseLong(properties.getProperty("couchbase.connectTimeout", "5"));
    long queryTimeout = Long.parseLong(properties.getProperty("couchbase.queryTimeout", "75"));

    ttlSeconds = Integer.parseInt(properties.getProperty("couchbase.ttlSeconds", "0"));

    if (sslMode) {
      couchbasePrefix = "couchbases://";
    } else {
      couchbasePrefix = "couchbase://";
    }

    String connectString = couchbasePrefix + hostname;

    synchronized (INIT_COORDINATOR) {
      try {
        if (environment == null) {
          Consumer<SecurityConfig.Builder> secConfiguration = securityConfig -> securityConfig
              .enableTls(sslMode)
              .enableHostnameVerification(false)
              .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);

          Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> ioConfig
              .numKvConnections(kvEndpoints)
              .networkResolution(NetworkResolution.AUTO)
              .enableMutationTokens(false);

          Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> timeoutConfig
              .kvTimeout(Duration.ofSeconds(kvTimeout))
              .connectTimeout(Duration.ofSeconds(connectTimeout))
              .queryTimeout(Duration.ofSeconds(queryTimeout));

          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(timeOutConfiguration)
              .ioConfig(ioConfiguration)
              .securityConfig(secConfiguration)
              .build();
          cluster = Cluster.connect(connectString,
              ClusterOptions.clusterOptions(username, password).environment(environment));
          bucket = cluster.bucket(bucketName);
          collection = bucket.scope(scopeName).collection(collectionName);
        }
      } catch(Exception e) {
        logError(e, connectString);
      }
    }

    OPEN_CLIENTS.incrementAndGet();
  }

  private void logError(Exception error, String connectString) {
    LOGGER.error(String.format("Connection string: %s", connectString));
    LOGGER.error(cluster.environment().toString());
    LOGGER.error(cluster.diagnostics().endpoints().toString());
    LOGGER.error(error.getMessage(), error);
  }

  private DurabilityLevel setDurabilityLevel(final int value) {
    switch(value){
      case 1:
        return DurabilityLevel.MAJORITY;
      case 2:
        return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
      case 3:
        return DurabilityLevel.PERSIST_TO_MAJORITY;
      default :
        return DurabilityLevel.NONE;
    }
  }

  @Override
  public synchronized void cleanup() {
  }

  private static <T>T retryBlock(Callable<T> block) throws Exception {
    int retryCount = 10;
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      try {
        return block.call();
      } catch (Exception e) {
        LOGGER.debug("Retry count: {} error: {}", retryCount, e.getMessage(), e);
        if (retryNumber == retryCount) {
          throw e;
        } else {
          double factor = waitFactor * retryNumber;
          long wait = (long) factor;
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    return block.call();
  }

  /**
   * Perform key/value read ("get").
   * @param table The name of the table.
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Map of field/value pairs for the result.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String fieldSpec = fields != null ? String.join(",", fields) : "*";
    String statement = "SELECT " + fieldSpec + " FROM " + keyspace() + " WHERE meta().id IN " + "[\"" + key + "\"]";
    try {
      return retryBlock(() -> {
        QueryResult response = cluster.query(statement, queryOptions()
            .adhoc(adhoc)
            .maxParallelism(maxParallelism)
            .retryStrategy(FailFastRetryStrategy.INSTANCE));
        JsonObject r = response.rowsAsObject().get(0);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(r.toString());
        }
        return response.metaData().status() == QueryStatus.SUCCESS ? Status.OK : Status.ERROR;
      });
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
    ObjectMapper mapper = new ObjectMapper();
    String json;

    ObjectNode contents = toJson(values);

    try {
      json = mapper.writeValueAsString(contents);
    } catch (JsonProcessingException e) {
      json = null;
    }

    if (json == null) {
      LOGGER.error("Update: Can not serialize values: {}", values);
      return Status.ERROR;
    }

    String statement = "UPSERT INTO " + keyspace() + " (KEY,VALUE) VALUES (\"" + key + "\"," + json + ")";
    try {
      return retryBlock(() -> {
        QueryResult result = cluster.query(statement, queryOptions()
            .adhoc(adhoc)
            .maxParallelism(maxParallelism)
            .retryStrategy(FailFastRetryStrategy.INSTANCE));
        return result.metaData().status() == QueryStatus.SUCCESS ? Status.OK : Status.ERROR;
      });
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
    ObjectMapper mapper = new ObjectMapper();
    String json;

    ObjectNode contents = toJson(values);
    contents.put("id", key);
    contents.put("record", recordNumber.incrementAndGet());

    try {
      json = mapper.writeValueAsString(contents);
    } catch (JsonProcessingException e) {
      json = null;
    }

    if (json == null) {
      LOGGER.error("Insert: Can not serialize values: {}", values);
      return Status.ERROR;
    }

    String statement = "UPSERT INTO " + keyspace() + " (KEY,VALUE) VALUES (\"" + key + "\"," + json + ")";
    try {
      return retryBlock(() -> {
        QueryResult result = cluster.query(statement, queryOptions()
            .adhoc(adhoc)
            .maxParallelism(maxParallelism)
            .retryStrategy(FailFastRetryStrategy.INSTANCE));
        return result.metaData().status() == QueryStatus.SUCCESS ? Status.OK : Status.ERROR;
      });
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
   * Helper method to turn the passed in iterator values into a map we can encode to json.
   *
   * @param values the values to encode.
   * @return the map of encoded values.
   */
  private static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>();
    values.entrySet()
        .parallelStream()
        .forEach(entry -> result.put(entry.getKey(), entry.getValue().toString()));
    return result;
  }

  private static ObjectNode toJson(final Map<String, ByteIterator> values) {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ByteIteratorSerializer");
    module.addSerializer(ByteIterator.class, new ByteIteratorSerializer());
    mapper.registerModule(module);
    return mapper.valueToTree(values);
  }

  private static String keyspace() {
    return bucketName + "." + scopeName + "." +collectionName;
  }

  /**
   * Remove a record.
   * @param table The name of the table.
   * @param key The record key of the record to delete.
   */
  @Override
  public Status delete(final String table, final String key) {
    try {
      return retryBlock(() -> {
        collection.remove(key);
        return Status.OK;
      });
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
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   */
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    Vector<Object> results = new Vector<>();
    try {
      return retryBlock(() -> {
        final String query = "select raw meta().id from `" + bucketName + "` where meta().id >= \"$1\" limit $2;";
        cluster.reactive().query(query, queryOptions()
                .pipelineBatch(128)
                .pipelineCap(1024)
                .scanCap(1024)
                .readonly(true)
                .adhoc(adhoc)
                .maxParallelism(maxParallelism)
                .retryStrategy(FailFastRetryStrategy.INSTANCE)
                .parameters(JsonArray.from(startkey, recordcount)))
            .flatMapMany(res -> res.rowsAs(String.class).parallel())
            .parallel()
            .subscribe(
                next -> results.add(collection.get(next, getOptions().transcoder(transcoder))
                    .contentAs(contentType)),
                error ->
                {
                  LOGGER.debug(error.getMessage(), error);
                  throw new RuntimeException(error);
                }
            );
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Scanned {} records", results.size());
        }
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }
}
