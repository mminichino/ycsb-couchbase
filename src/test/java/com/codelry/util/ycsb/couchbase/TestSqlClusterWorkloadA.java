package com.codelry.util.ycsb.couchbase;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live-cluster SQL record store smoke test for workload A using {@code CB_HOST},
 * {@code CB_USERNAME}, and {@code CB_PASSWORD} from the environment
 * (typically loaded from {@code .env} by Gradle).
 */
@EnabledIfEnvironmentVariable(named = "CB_HOST", matches = ".+")
@EnabledIfEnvironmentVariable(named = "CB_USERNAME", matches = ".+")
@EnabledIfEnvironmentVariable(named = "CB_PASSWORD", matches = ".+")
class TestSqlClusterWorkloadA {

  @Test
  void runWorkloadAWithSqlRecordStore() {
    String host = System.getenv("CB_HOST");
    String username = System.getenv("CB_USERNAME");
    String password = System.getenv("CB_PASSWORD");
    Assumptions.assumeFalse(host == null || host.isBlank(), "CB_HOST must be set");
    Assumptions.assumeFalse(username == null || username.isBlank(), "CB_USERNAME must be set");
    Assumptions.assumeFalse(password == null || password.isBlank(), "CB_PASSWORD must be set");

    SqlClusterWorkloadSupport.runWorkload("a", host, username, password);
  }
}
