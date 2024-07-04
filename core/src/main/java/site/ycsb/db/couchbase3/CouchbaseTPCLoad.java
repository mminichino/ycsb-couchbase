package site.ycsb.db.couchbase3;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.analytics.AnalyticsIndexManager;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.query.CollectionQueryIndexManager;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.manager.analytics.CreateDatasetAnalyticsOptions.createDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDataverseAnalyticsOptions.createDataverseAnalyticsOptions;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;
import site.ycsb.Status;
import site.ycsb.TableKeys;
import site.ycsb.tpc.tpcc.*;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;

public class CouchbaseTPCLoad extends LoadDriver {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseTPCLoad");
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
  private boolean defaultScope;
  private static final AtomicLong recordNumber = new AtomicLong(0);
  private final List<Future<MutationResult>> tasks = new ArrayList<>();
  private ExecutorService executor = Executors.newFixedThreadPool(32);
  private boolean analyticsMode;
  private boolean debug;

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
    debug = properties.getProperty("couchbase.debug", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    analyticsMode = properties.getProperty("couchbase.analytics", "false").equals("true");
    defaultScope = properties.getProperty("couchbase.defaultScope", "false").equals("true");

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

  public void taskAdd(Callable<MutationResult> task) {
    tasks.add(executor.submit(task));
  }

  public boolean taskWait() {
    boolean status = true;
    for (Future<MutationResult> future : tasks) {
      try {
        MutationResult result = future.get();
        if (debug) {
          LOGGER.debug("Task status: {}", result);
        }
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error(e.getMessage(), e);
        status = false;
      }
    }
    tasks.clear();
    return status;
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
      if (analyticsMode) {
        createAnalyticsCollection(name);
      }
      for (String field : indexFields) {
        createFieldIndex(name, field);
      }
    } catch (CollectionExistsException e) {
      LOGGER.debug("Collection {} already exists", name);
    } catch (Throwable t) {
      LOGGER.error("createTable transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public void createAnalyticsCollection(String name) {
    String statement;

    statement = "CREATE ANALYTICS SCOPE " + bucketName + "." + scopeName + " IF NOT EXISTS";
    runQuery(statement);
    statement = "CREATE ANALYTICS COLLECTION IF NOT EXISTS " + bucketName + "." + scopeName + "." + name + " ON " + bucketName + "." + scopeName + "." + name;
    runQuery(statement);
  }

  @Override
  public Status createItemTable() {
    System.out.println("Creating item table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(itemTable.primaryKeyName);
    indexFields.addAll(itemTable.foreignKeyNames);
    return createCollection("item", indexFields);
  }

  @Override
  public Status createWarehouseTable() {
    System.out.println("Creating warehouse table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(warehouseTable.primaryKeyName);
    indexFields.addAll(warehouseTable.foreignKeyNames);
    return createCollection("warehouse", indexFields);
  }

  @Override
  public Status createStockTable() {
    System.out.println("Creating stock table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(stockTable.primaryKeyName);
    indexFields.addAll(stockTable.foreignKeyNames);
    return createCollection("stock", indexFields);
  }

  @Override
  public Status createDistrictTable() {
    System.out.println("Creating district table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(districtTable.primaryKeyName);
    indexFields.addAll(districtTable.foreignKeyNames);
    return createCollection("district", indexFields);
  }

  @Override
  public Status createCustomerTable() {
    System.out.println("Creating customer table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(customerTable.primaryKeyName);
    indexFields.addAll(customerTable.foreignKeyNames);
    return createCollection("customer", indexFields);
  }

  @Override
  public Status createHistoryTable() {
    System.out.println("Creating history table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(historyTable.primaryKeyName);
    indexFields.addAll(historyTable.foreignKeyNames);
    return createCollection("history", indexFields);
  }

  @Override
  public Status createOrderTable() {
    System.out.println("Creating order table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(orderTable.primaryKeyName);
    indexFields.addAll(orderTable.foreignKeyNames);
    return createCollection("orders", indexFields);
  }

  @Override
  public Status createNewOrderTable() {
    System.out.println("Creating new order table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(newOrderTable.primaryKeyName);
    indexFields.addAll(newOrderTable.foreignKeyNames);
    return createCollection("new_orders", indexFields);
  }

  @Override
  public Status createOrderLineTable() {
    System.out.println("Creating order line table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(orderLineTable.primaryKeyName);
    indexFields.addAll(orderLineTable.foreignKeyNames);
    return createCollection("order_line", indexFields);
  }

  @Override
  public Status createSupplierTable() {
    System.out.println("Creating supplier table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(supplierTable.primaryKeyName);
    indexFields.addAll(supplierTable.foreignKeyNames);
    return createCollection("supplier", indexFields);
  }

  @Override
  public Status createNationTable() {
    System.out.println("Creating nation table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(nationTable.primaryKeyName);
    indexFields.addAll(nationTable.foreignKeyNames);
    return createCollection("nation", indexFields);
  }

  @Override
  public Status createRegionTable() {
    System.out.println("Creating region table");
    List<String> indexFields = new ArrayList<>();
    indexFields.add(regionTable.primaryKeyName);
    indexFields.addAll(regionTable.foreignKeyNames);
    return createCollection("region", indexFields);
  }

  private String keyspace(String collection) {
    return bucketName + "." + scopeName + "." + collection;
  }

  public Status runQuery(String statement) {
    try {
      return retryBlock(() -> {
        cluster.analyticsQuery(statement, analyticsOptions());
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("query exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public Status insertRecords(String collection, String json) {
    String statement = "UPSERT INTO " + keyspace(collection) + " (" + json + ")";
    try {
      return retryBlock(() -> {
        cluster.analyticsQuery(statement, analyticsOptions());
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public MutationResult insert(Collection collection, String id, ObjectNode record) {
    try {
    return retryBlock(() -> collection.upsert(id, record, upsertOptions().timeout(Duration.ofSeconds(10))));
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return null;
    }
  }

  @Override
  public void insertItemBatch(List<Item> batch) {
    LOGGER.info("insertItemBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("item");
    for (Item i : batch) {
      ObjectNode record = i.asNode();
      String id = itemTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertWarehouseBatch(List<Warehouse> batch) {
    LOGGER.info("insertWarehouseBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("warehouse");
    for (Warehouse i : batch) {
      ObjectNode record = i.asNode();
      String id = warehouseTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertStockBatch(List<Stock> batch) {
    LOGGER.info("insertStockBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("stock");
    for (Stock i : batch) {
      ObjectNode record = i.asNode();
      String id = stockTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertDistrictBatch(List<District> batch) {
    LOGGER.info("insertDistrictBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("district");
    for (District i : batch) {
      ObjectNode record = i.asNode();
      String id = districtTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertCustomerBatch(List<Customer> batch) {
    LOGGER.info("insertCustomerBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("customer");
    for (Customer i : batch) {
      ObjectNode record = i.asNode();
      String id = customerTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertHistoryBatch(List<History> batch) {
    LOGGER.info("insertHistoryBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("history");
    for (History i : batch) {
      ObjectNode record = i.asNode();
      String id = historyTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertOrderBatch(List<Order> batch) {
    LOGGER.info("insertOrderBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("order");
    for (Order i : batch) {
      ObjectNode record = i.asNode();
      String id = orderTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertNewOrderBatch(List<NewOrder> batch) {
    LOGGER.info("insertNewOrderBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("new_orders");
    for (NewOrder i : batch) {
      ObjectNode record = i.asNode();
      String id = newOrderTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertOrderLineBatch(List<OrderLine> batch) {
    LOGGER.info("insertOrderLineBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("order_line");
    for (OrderLine i : batch) {
      ObjectNode record = i.asNode();
      String id = orderLineTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertSupplierBatch(List<Supplier> batch) {
    LOGGER.info("insertSupplierBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("supplier");
    for (Supplier i : batch) {
      ObjectNode record = i.asNode();
      String id = supplierTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertNationBatch(List<Nation> batch) {
    LOGGER.info("insertNationBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("nation");
    for (Nation i : batch) {
      ObjectNode record = i.asNode();
      String id = nationTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }

  @Override
  public void insertRegionBatch(List<Region> batch) {
    LOGGER.info("insertRegionBatch: called with {} items", batch.size());
    Collection collection = bucket.scope(scopeName).collection("region");
    for (Region i : batch) {
      ObjectNode record = i.asNode();
      String id = regionTable.getDocumentId(record);
      taskAdd(() -> insert(collection, id, record));
    }
    taskWait();
  }
}
