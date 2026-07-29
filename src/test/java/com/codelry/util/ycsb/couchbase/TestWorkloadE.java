package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.RunBenchmark;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Runs workload E end-to-end the way ycsb-core does: test setup, load, then transactions.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestWorkloadE {

  private static final String[] ARGS = {
      "-w", "workloade"
  };

  @Test
  @Order(1)
  void testWorkloadE() {
    assertDoesNotThrow(() -> RunBenchmark.main(ARGS));
  }
}
