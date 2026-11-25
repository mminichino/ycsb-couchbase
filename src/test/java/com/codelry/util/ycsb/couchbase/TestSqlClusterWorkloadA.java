package com.codelry.util.ycsb.couchbase;

import org.junit.jupiter.api.Test;

/**
 * SQL record store smoke test for workload A against a Couchbase Server Testcontainer.
 * Connection settings come from {@code db.properties}.
 */
class TestSqlClusterWorkloadA extends AbstractServerPerTestTestcontainerTest {

  @Test
  void runWorkloadAWithSqlRecordStore() {
    SqlClusterWorkloadSupport.runWorkload("a");
  }
}
