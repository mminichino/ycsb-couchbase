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
import com.couchbase.client.core.env.*;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

/**
 * A class that implements the Couchbase Java SDK to be used with YCSB.
 */
public class Couchbase3SQLClient extends DB {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.Couchbase3SQLClient");
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  public static final String COUCHBASE_HOST = "couchbase.hostname";
  public static final String COUCHBASE_USER = "couchbase.username";
  public static final String COUCHBASE_PASSWORD = "couchbase.password";
  public static final String COUCHBASE_CLIENT_CERTIFICATE = "couchbase.client.cert";
  public static final String COUCHBASE_ROOT_CERTIFICATE = "couchbase.ca.cert";
  public static final String COUCHBASE_KEYSTORE_TYPE = "couchbase.keystore.type";
  public static final String COUCHBASE_SSL_MODE = "couchbase.sslMode";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  public static final String INDEX_CREATE = "index.create";
  public static final String INDEX_CREATE_DEFAULT = "false";
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Cluster cluster;
  private static volatile Bucket bucket;
  private static volatile ClusterEnvironment environment;
  private static String keySpace;
  private static boolean adhoc;
  private static String allFields;
  private static int maxParallelism;

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
    String clientCert = properties.getProperty(COUCHBASE_CLIENT_CERTIFICATE);
    String rootCert = properties.getProperty(COUCHBASE_ROOT_CERTIFICATE);
    KeyStoreType keyStoreType = KeyStoreType.valueOf(properties.getProperty(COUCHBASE_KEYSTORE_TYPE, "PKCS12").toUpperCase());
    String bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    String scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    String collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    boolean sslMode = properties.getProperty(COUCHBASE_SSL_MODE, "false").equals("true");
    boolean debug = getProperties().getProperty("couchbase.debug", "false").equals("true");
    keySpace = bucketName + "." + scopeName + "." + collectionName;

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));
    int kvEndpoints = Integer.parseInt(properties.getProperty("couchbase.kvEndpoints", "8"));
    long kvTimeout = Long.parseLong(properties.getProperty("couchbase.kvTimeout", "10"));
    long connectTimeout = Long.parseLong(properties.getProperty("couchbase.connectTimeout", "10"));
    long queryTimeout = Long.parseLong(properties.getProperty("couchbase.queryTimeout", "75"));

    int fieldCount = Integer.parseInt(properties.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    StringBuilder fieldBuilder = new StringBuilder();
    fieldBuilder.append("field0");
    for (int idx = 1; idx < fieldCount; idx++) {
      fieldBuilder.append(",field");
      fieldBuilder.append(idx);
    }
    allFields = fieldBuilder.toString();

    if (sslMode) {
      couchbasePrefix = "couchbases://";
    } else {
      couchbasePrefix = "couchbase://";
    }

    String connectString = couchbasePrefix + hostname;

    synchronized (INIT_COORDINATOR) {
      try {
        if (environment == null) {
          Consumer<SecurityConfig.Builder> secConfiguration;
          if (rootCert != null) {
            secConfiguration = securityConfig -> securityConfig
                .enableTls(true)
                .trustCertificate(Paths.get(rootCert));
          } else {
            secConfiguration = securityConfig -> securityConfig
                .enableTls(sslMode)
                .enableHostnameVerification(false)
                .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
          }

          Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> ioConfig
              .numKvConnections(kvEndpoints)
              .networkResolution(NetworkResolution.AUTO)
              .enableMutationTokens(false);

          Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> timeoutConfig
              .kvTimeout(Duration.ofSeconds(kvTimeout))
              .connectTimeout(Duration.ofSeconds(connectTimeout))
              .queryTimeout(Duration.ofSeconds(queryTimeout));

          Authenticator authenticator;
          if (clientCert != null) {
            KeyStore keyStore = KeyStore.getInstance(keyStoreType.name());
            keyStore.load(new FileInputStream(clientCert), password.toCharArray());
            authenticator = CertificateAuthenticator.fromKeyStore(
                keyStore,
                password
            );
          } else {
            authenticator = PasswordAuthenticator.create(username, password);
          }

          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(timeOutConfiguration)
              .ioConfig(ioConfiguration)
              .securityConfig(secConfiguration)
              .build();
          cluster = Cluster.connect(connectString,
              ClusterOptions.clusterOptions(authenticator).environment(environment));
          bucket = cluster.bucket(bucketName);
          bucket.waitUntilReady(Duration.ofSeconds(5));
        }
      } catch(Exception e) {
        logError(e, connectString);
        throw new DBException(e);
      }
    }

    OPEN_CLIENTS.incrementAndGet();
  }

  private void logError(Exception error, String connectString) {
    LOGGER.error("Connection string: {}", connectString);
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

  /**
   * Perform key/value read ("get").
   * @param table The name of the table.
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Map of field/value pairs for the result.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    final String query = "SELECT " + allFields + " FROM " + keySpace + " WHERE id = ?";
    MapSerializer serializer = new MapSerializer();
    try {
      result = cluster.reactive().query(query, queryOptions()
              .pipelineBatch(256)
              .pipelineCap(1024)
              .scanCap(1024)
              .maxParallelism(maxParallelism)
              .readonly(true)
              .adhoc(adhoc)
              .serializer(serializer)
              .parameters(JsonArray.from(key)))
          .flatMapMany(r -> r.rowsAs(Map.class))
          .take(1)
          .blockLast();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Row (read): {}", result != null ? result.toString() : "null");
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
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ByteIteratorSerializer");
    module.addSerializer(ByteIterator.class, new ByteIteratorSerializer());
    mapper.registerModule(module);
    try {
      values.put("id", new StringByteIterator(key));
      String json = mapper.writeValueAsString(values);
      final String query = "UPSERT INTO " + keySpace + "(KEY, VALUE) VALUES (\"" + key + "\", " + json + ")";
      cluster.query(query, queryOptions()
          .adhoc(adhoc)
          .maxParallelism(maxParallelism));
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
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ByteIteratorSerializer");
    module.addSerializer(ByteIterator.class, new ByteIteratorSerializer());
    mapper.registerModule(module);
    try {
      values.put("id", new StringByteIterator(key));
      String json = mapper.writeValueAsString(values);
      final String query = "UPSERT INTO " + keySpace + "(KEY, VALUE) VALUES (\"" + key + "\", " + json + ")";
      cluster.query(query, queryOptions()
          .adhoc(adhoc)
          .maxParallelism(maxParallelism));
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
      final String query = "DELETE FROM " + keySpace + " WHERE id = ?";
      cluster.query(query, queryOptions()
          .adhoc(adhoc)
          .maxParallelism(maxParallelism)
          .parameters(JsonArray.from(key)));
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
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   */
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    final String query = "SELECT " + allFields + " FROM " + keySpace + " WHERE id >= ? LIMIT ?";
    try {
      List<JsonObject> data = cluster.reactive().query(query, queryOptions()
              .pipelineBatch(256)
              .pipelineCap(1024)
              .scanCap(1024)
              .maxParallelism(maxParallelism)
              .readonly(true)
              .adhoc(adhoc)
              .parameters(JsonArray.from(startkey, recordcount)))
          .flatMapMany(ReactiveQueryResult::rowsAsObject)
          .collectList()
          .block();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Scanned {} records", data != null ? data.size(): 0);
      }
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }
}
