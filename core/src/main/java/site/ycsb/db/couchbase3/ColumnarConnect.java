package site.ycsb.db.couchbase3;

public class ColumnarConnect {
  private final CouchbaseConnect db;

  public ColumnarConnect(String hostname, String username, String password, String bucket, String scope) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    dbBuilder.connect(hostname, username, password)
        .ssl(true)
        .columnar(true)
        .bucket(bucket)
        .scope(scope);
    this.db = dbBuilder.build();
  }

}
