package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.RunBenchmark;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Runs workload A end-to-end the way ycsb-core does: test setup, load, then transactions.
 */
class TestWorkloadA extends AbstractServerPerTestTestcontainerTest {

  private static final String[] ARGS = {
      "-w", "workloada"
  };

  @Test
  void testWorkloadA() {
    assertDoesNotThrow(() -> RunBenchmark.main(ARGS));
  }
}
