package site.ycsb.db.couchbase3;

import site.ycsb.TestCleanup;
import java.util.Properties;

import com.codelry.util.cbdb3.CouchbaseConnect;
import com.codelry.util.cbdb3.CouchbaseConfig;

/**
 * Clean Cluster after Testing.
 */
public class CouchbaseTestCleanup extends TestCleanup {

  @Override
  public void testClean(Properties properties) {
    CouchbaseConnect db = CouchbaseConnect.getInstance();
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
    db.connect(config);

    try {
      System.err.printf("Removing bucket %s on cluster:[%s]\n", db.getBucketName(), db.hostValue());
      db.dropBucket();
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
