package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.RunBenchmark;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Runs workload E end-to-end the way ycsb-core does: test setup, load, then transactions.
 */
class TestWorkloadE extends AbstractServerPerTestTestcontainerTest {

  private static final String[] ARGS = {
      "-w", "workloade"
  };

  @Test
  void testWorkloadE() {
    assertDoesNotThrow(() -> RunBenchmark.main(ARGS));
  }
}
