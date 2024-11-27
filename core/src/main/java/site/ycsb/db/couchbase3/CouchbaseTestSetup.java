package site.ycsb.db.couchbase3;

import site.ycsb.TestSetup;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.codelry.util.cbdb3.CouchbaseConnect;
import com.codelry.util.cbdb3.CouchbaseConfig;

/**
 * Prepare Cluster for Testing.
 */
public class CouchbaseTestSetup extends TestSetup {
  public static final String INDEX_CREATE = "index.create";
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  @Override
  public void testSetup(Properties properties) {
    boolean index = properties.getProperty(INDEX_CREATE, "false").equals("true");
    int fieldCount = Integer.parseInt(properties.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    String indexName = "ycsb_fields_idx";

    CouchbaseConnect db = CouchbaseConnect.getInstance();
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
    db.connect(config);

    try {
      System.err.printf("Creating bucket %s (%s) on cluster:[%s]\n", db.getBucketName(), config.getBucketStorage().toString(), db.hostValue());
      db.createBucket();
      System.err.printf("Creating scope %s\n", db.getScopeName());
      db.createScope();
      System.err.printf("Creating collection %s\n", db.getCollectionName());
      db.createCollection();
      if (index) {
        List<String> allFields = new ArrayList<>();
        allFields.add("id");
        for (int i = 0; i < fieldCount; i++) {
          allFields.add("field" + i);
        }
        System.err.printf("Creating index %s on %s\n", indexName, db.getCollectionName());
        db.createSecondaryIndex(indexName, allFields);
      }
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
