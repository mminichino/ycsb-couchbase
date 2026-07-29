package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.RunBenchmark;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Runs workload A end-to-end the way ycsb-core does: test setup, load, then transactions.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestWorkloadA {

  private static final String[] ARGS = {
      "-w", "workloada"
  };

  @Test
  @Order(1)
  void testWorkloadA() {
    assertDoesNotThrow(() -> RunBenchmark.main(ARGS));
  }
}
