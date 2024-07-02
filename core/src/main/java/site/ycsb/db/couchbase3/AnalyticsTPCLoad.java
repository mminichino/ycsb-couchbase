package site.ycsb.db.couchbase3;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.query.CollectionQueryIndexManager;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

import site.ycsb.Status;
import site.ycsb.tpc.tpcc.*;

import java.util.List;

public class AnalyticsTPCLoad extends LoadDriver {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.AnalyticsTPCLoad");
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
  private static volatile Scope scope;
  private static volatile CollectionManager collectionManager;
  private static volatile ClusterEnvironment environment;
  private static String bucketName;
  private static String scopeName;
  private static String collectionName;
  private boolean adhoc;
  private int maxParallelism;
  private boolean analyticsMode;
  private static final AtomicLong recordNumber = new AtomicLong(0);

  @Override
  public void init() {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();
    String couchbasePrefix;

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        properties.load(propFile.openStream());
      } catch (IOException e) {
        throw new RuntimeException(e);
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
    boolean debug = properties.getProperty("couchbase.debug", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    analyticsMode = properties.getProperty("couchbase.analytics", "false").equals("true");

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));
    int kvEndpoints = Integer.parseInt(properties.getProperty("couchbase.kvEndpoints", "4"));

    long kvTimeout = Long.parseLong(properties.getProperty("couchbase.kvTimeout", "5"));
    long connectTimeout = Long.parseLong(properties.getProperty("couchbase.connectTimeout", "5"));
    long queryTimeout = Long.parseLong(properties.getProperty("couchbase.queryTimeout", "75"));

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
          scope = bucket.scope(scopeName);
          collectionManager = bucket.collections();
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

  public void createFieldIndex(String collectionName, String field) {
    Collection collection = bucket.scope(scopeName).collection(collectionName);
    CollectionQueryIndexManager queryIndexMgr = collection.queryIndexes();
    CreateQueryIndexOptions options = CreateQueryIndexOptions.createQueryIndexOptions()
        .deferred(false)
        .numReplicas(1)
        .ignoreIfExists(true);

    String indexName = "idx_" + field.replaceAll("\\(\\).", "");
    queryIndexMgr.createIndex(indexName, Collections.singletonList(field), options);
    queryIndexMgr.watchIndexes(Collections.singletonList(indexName), Duration.ofSeconds(10));
  }

  public Status createCollection(String name, List<String> indexFields) {
    try {
      LOGGER.debug("Creating collection: {}", name);
      collectionManager.createCollection(scopeName, name);
      for (String field : indexFields) {
        createFieldIndex(name, field);
      }
      return Status.OK;
    } catch (CollectionExistsException e) {
      LOGGER.debug("Collection {} already exists", name);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("createTable transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status createItemTable() {
    LOGGER.info("Creating item table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(itemTable.primaryKeyName);
    indexFields.addAll(itemTable.foreignKeyNames);
    return createCollection("item", indexFields);
  }

  @Override
  public Status createWarehouseTable() {
    LOGGER.info("Creating warehouse table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(warehouseTable.primaryKeyName);
    indexFields.addAll(warehouseTable.foreignKeyNames);
    return createCollection("warehouse", indexFields);
  }

  @Override
  public Status createStockTable() {
    LOGGER.info("Creating stock table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(stockTable.primaryKeyName);
    indexFields.addAll(stockTable.foreignKeyNames);
    return createCollection("stock", indexFields);
  }

  @Override
  public Status createDistrictTable() {
    LOGGER.info("Creating district table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(districtTable.primaryKeyName);
    indexFields.addAll(districtTable.foreignKeyNames);
    return createCollection("district", indexFields);
  }

  @Override
  public Status createCustomerTable() {
    LOGGER.info("Creating customer table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(customerTable.primaryKeyName);
    indexFields.addAll(customerTable.foreignKeyNames);
    return createCollection("customer", indexFields);
  }

  @Override
  public Status createHistoryTable() {
    LOGGER.info("Creating history table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(historyTable.primaryKeyName);
    indexFields.addAll(historyTable.foreignKeyNames);
    return createCollection("history", indexFields);
  }

  @Override
  public Status createOrderTable() {
    LOGGER.info("Creating order table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(orderTable.primaryKeyName);
    indexFields.addAll(orderTable.foreignKeyNames);
    return createCollection("orders", indexFields);
  }

  @Override
  public Status createNewOrderTable() {
    LOGGER.info("Creating new order table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(newOrderTable.primaryKeyName);
    indexFields.addAll(newOrderTable.foreignKeyNames);
    return createCollection("new_orders", indexFields);
  }

  @Override
  public Status createOrderLineTable() {
    LOGGER.info("Creating order line table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(orderLineTable.primaryKeyName);
    indexFields.addAll(orderLineTable.foreignKeyNames);
    return createCollection("order_line", indexFields);
  }

  @Override
  public void insertItemBatch(List<Item> batch) {

  }

  @Override
  public void insertWarehouseBatch(List<Warehouse> batch) {

  }

  @Override
  public void insertStockBatch(List<Stock> batch) {

  }

  @Override
  public void insertDistrictBatch(List<District> batch) {

  }

  @Override
  public void insertCustomerBatch(List<Customer> batch) {

  }

  @Override
  public void insertHistoryBatch(List<History> batch) {

  }

  @Override
  public void insertOrderBatch(List<Order> batch) {

  }

  @Override
  public void insertNewOrderBatch(List<NewOrder> batch) {

  }

  @Override
  public void insertOrderLineBatch(List<OrderLine> batch) {

  }
}