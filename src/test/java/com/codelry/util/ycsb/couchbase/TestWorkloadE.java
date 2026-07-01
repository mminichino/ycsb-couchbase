package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.Benchmark;
import org.junit.jupiter.api.*;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Runs workload E end-to-end the way ycsb-core does: test setup, load, then transactions.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestWorkloadE {

  private static final String[] LOAD_ARGS = {
      "-db", "com.codelry.util.ycsb.couchbase.CouchbaseClientBinding",
      "-P", "workloads/workloade",
      "-threads", "256",
      "-p", "writeallfields=true",
      "-p", "recordcount=1000000",
      "-s",
      "-load"
  };

  private static final String[] RUN_ARGS = {
      "-db", "com.codelry.util.ycsb.couchbase.CouchbaseClientBinding",
      "-P", "workloads/workloade",
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
  void loadWorkloadE() {
    Properties props = YcsbCliProperties.forBenchmark(LOAD_ARGS);
    assertDoesNotThrow(() -> new Benchmark().run(props));
  }

  @Test
  @Order(2)
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void runWorkloadE() {
    Properties props = YcsbCliProperties.forBenchmark(RUN_ARGS);
    assertDoesNotThrow(() -> new Benchmark().run(props));
  }
}
