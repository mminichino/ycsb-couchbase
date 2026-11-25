package com.codelry.util.ycsb.couchbase;

import org.junit.jupiter.api.Test;

/**
 * SQL record store smoke test for workload E against a Couchbase Server Testcontainer.
 * Connection settings come from {@code db.properties}.
 */
class TestSqlClusterWorkloadE extends AbstractServerPerTestTestcontainerTest {

  @Test
  void runWorkloadEWithSqlRecordStore() {
    SqlClusterWorkloadSupport.runWorkload("e");
  }
}
