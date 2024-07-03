package site.ycsb.db.couchbase3;

import site.ycsb.TestSetup;

import java.util.Properties;

/**
 * Prepare Cluster for Testing.
 */
public class CouchbaseTestSetup extends TestSetup {
  public static final String ENABLE_SETUP = "setup.enable";
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_SCOPE = "couchbase.scope";
  public static final String CLUSTER_COLLECTION = "couchbase.collection";
  public static final String CLUSTER_PROJECT = "couchbase.project";
  public static final String CLUSTER_DATABASE = "couchbase.database";
  public static final String CLUSTER_EVENTING = "couchbase.eventing";
  public static final String CLUSTER_BUCKET_TYPE = "couchbase.bucketType";
  public static final String CLUSTER_REPLICA_NUM = "couchbase.replicaNum";
  public static final String XDCR_HOST = "xdcr.hostname";
  public static final String XDCR_USER = "xdcr.username";
  public static final String XDCR_PASSWORD = "xdcr.password";
  public static final String XDCR_SSL = "xdcr.sslMode";
  public static final String XDCR_BUCKET = "xdcr.bucket";
  public static final String XDCR_SCOPE = "xdcr.scope";
  public static final String XDCR_COLLECTION = "xdcr.collection";
  public static final String XDCR_PROJECT = "xdcr.project";
  public static final String XDCR_DATABASE = "xdcr.database";
  public static final String XDCR_EVENTING = "xdcr.eventing";
  public static final String XDCR_BUCKET_TYPE = "xdcr.bucketType";
  public static final String XDCR_REPLICA_NUM = "xdcr.replicaNum";
  public static final String INDEX_CREATE = "index.create";
  public static final String INDEX_FIELD = "index.field";

  @Override
  public void testSetup(Properties properties) {
    boolean runSetup = Boolean.parseBoolean(properties.getProperty(ENABLE_SETUP, String.valueOf(true)));

    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "ycsb");
    String clusterScope = properties.getProperty(CLUSTER_SCOPE, "_default");
    String clusterCollection = properties.getProperty(CLUSTER_COLLECTION, "usertable");
    String clusterProject = properties.getProperty(CLUSTER_PROJECT, null);
    String clusterDatabase = properties.getProperty(CLUSTER_DATABASE, null);
    String clusterEventing = properties.getProperty(CLUSTER_EVENTING, null);
    int clusterBucketType = Integer.parseInt(properties.getProperty(CLUSTER_BUCKET_TYPE, "0"));
    int clusterReplicaNum = Integer.parseInt(properties.getProperty(CLUSTER_REPLICA_NUM, "1"));

    String xdcrHost = properties.getProperty(XDCR_HOST, null);
    String xdcrUser = properties.getProperty(XDCR_USER, CouchbaseConnect.DEFAULT_USER);
    String xdcrPassword = properties.getProperty(XDCR_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean xdcrSsl = properties.getProperty(XDCR_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String xdcrBucket = properties.getProperty(XDCR_BUCKET, "ycsb");
    String xdcrScope = properties.getProperty(XDCR_SCOPE, "_default");
    String xdcrCollection = properties.getProperty(XDCR_COLLECTION, "usertable");
    String xdcrProject = properties.getProperty(XDCR_PROJECT, null);
    String xdcrDatabase = properties.getProperty(XDCR_DATABASE, null);
    String xdcrEventing = properties.getProperty(XDCR_EVENTING, null);
    int xdcrBucketType = Integer.parseInt(properties.getProperty(XDCR_BUCKET_TYPE, "0"));
    int xdcrReplicaNum = Integer.parseInt(properties.getProperty(XDCR_REPLICA_NUM, "1"));

    boolean indexCreate = properties.getProperty(INDEX_CREATE, "false").equals("true");
    String indexField = properties.getProperty(INDEX_FIELD, "meta().id");

    if (!runSetup) {
      System.out.println("Skipping setup automation");
      return;
    }

    System.err.println("Starting test setup");

    clusterSetup(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket, clusterScope, clusterCollection,
        clusterBucketType, clusterReplicaNum, clusterProject, clusterDatabase, indexCreate, indexField, clusterEventing);

    if (clusterEventing != null) {
      eventingSetup(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket, clusterEventing);
    }

    if (xdcrHost != null) {
      clusterSetup(xdcrHost, xdcrUser, xdcrPassword, xdcrSsl, xdcrBucket, xdcrScope, xdcrCollection, xdcrBucketType,
          xdcrReplicaNum, xdcrProject, xdcrDatabase, indexCreate, indexField, xdcrEventing);
      replicationSetup(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket,
          xdcrHost, xdcrUser, xdcrPassword, xdcrSsl, xdcrBucket);
    }
  }

  private static void clusterSetup(String host, String user, String password, boolean ssl, String bucket, String scope,
                                   String collection, int type, int replicas, String project, String database,
                                   boolean index, String field, String eventing) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;
    String typeText = type == 1 ? "Magma" : "Couchstore";

    try {
      dbBuilder.connect(host, user, password)
          .ssl(ssl)
          .bucket(bucket);
      if (project != null && database != null) {
        dbBuilder.capella(project, database);
      }
      db = dbBuilder.build();
      if (eventing != null) {
        System.err.printf("Creating eventing bucket on cluster:[%s]\n", host);
        db.createBucket("eventing", 128L, replicas);
      }
      System.err.printf("Creating bucket %s (%s) on cluster:[%s]\n", bucket, typeText, host);
      db.createBucket(bucket, replicas, type);
      System.err.printf("Creating scope %s\n", scope);
      db.createScope(bucket, scope);
      System.err.printf("Creating collection %s\n", collection);
      db.createCollection(bucket, scope, collection);
      if (db.isAnalyticsEnabled()) {
        System.err.printf("Creating analytics collection %s\n", bucket);
        db.createAnalyticsCollection(bucket);
        System.err.println("Creating analytics indexes");
        db.createAnalyticsIntIndex(bucket, "record");
        db.createAnalyticsStrIndex(bucket, "id");
        db.createFieldIndex("record");
        db.createFieldIndex("id");
      }
      if (index) {
        System.err.printf("Creating index on field %s\n", field);
        db.createFieldIndex(field);
      }
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void replicationSetup(String sourceHost, String sourceUser, String sourcePassword,
                                       boolean sourceSsl, String sourceBucket,
                                       String targetHost, String targetUser, String targetPassword,
                                       boolean targetSsl, String targetBucket) {
    CouchbaseXDCR.XDCRBuilder xdcrBuilder = new CouchbaseXDCR.XDCRBuilder();
    CouchbaseConnect.CouchbaseBuilder sourceBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect.CouchbaseBuilder targetBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect sourceDb;
    CouchbaseConnect targetDb;

    try {
      sourceBuilder.connect(sourceHost, sourceUser, sourcePassword)
          .ssl(sourceSsl)
          .bucket(sourceBucket);
      sourceDb = sourceBuilder.build();
      targetBuilder.connect(targetHost, targetUser, targetPassword)
          .ssl(targetSsl)
          .bucket(targetBucket);
      targetDb = targetBuilder.build();

      System.err.printf("Replicating %s:%s -> %s:%s\n", sourceHost, sourceBucket, targetHost, targetBucket);
      CouchbaseXDCR xdcr = xdcrBuilder.source(sourceDb).target(targetDb).build();
      xdcr.createReplication();

      sourceDb.disconnect();
      targetDb.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void eventingSetup(String host, String user, String password, boolean ssl, String bucket,
                                    String resource) {
    CouchbaseEventing.EventingBuilder eventingBuilder = new CouchbaseEventing.EventingBuilder();
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;

    try {
      dbBuilder.connect(host, user, password)
              .ssl(ssl)
              .bucket(bucket);
      db = dbBuilder.build();
      CouchbaseEventing eventing = eventingBuilder.database(db).build();
      System.err.printf("Deploying eventing function %s on cluster:[%s]\n", resource, host);
      eventing.deployEventingFunction(resource, "eventing");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
