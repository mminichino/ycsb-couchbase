package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.Benchmark;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Runs workload A end-to-end the way ycsb-core does: test setup, load, then transactions.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestWorkloadA {

  private static final String[] LOAD_ARGS = {
      "-db", "com.codelry.util.ycsb.couchbase.CouchbaseClientBinding",
      "-P", "workloads/workloada",
      "-threads", "256",
      "-p", "writeallfields=true",
      "-p", "recordcount=1000000",
      "-s",
      "-load"
  };

  private static final String[] RUN_ARGS = {
      "-db", "com.codelry.util.ycsb.couchbase.CouchbaseClientBinding",
      "-P", "workloads/workloada",
      "-threads", "256",
      "-p", "writeallfields=true",
      "-p", "recordcount=1000000",
      "-p", "operationcount=10000000",
      "-p", "maxexecutiontime=120",
      "-s",
      "-t"
  };

  @Test
  @Order(1)
  void loadWorkloadA() {
    Properties props = YcsbCliProperties.forBenchmark(LOAD_ARGS);
    assertDoesNotThrow(() -> new Benchmark().run(props));
  }

  @Test
  @Order(2)
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void runWorkloadA() {
    Properties props = YcsbCliProperties.forBenchmark(RUN_ARGS);
    assertDoesNotThrow(() -> new Benchmark().run(props));
  }
}
