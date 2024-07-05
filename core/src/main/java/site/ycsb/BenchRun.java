package site.ycsb;

import java.util.*;

/**
 * Benchmark Driver
 */
public abstract class BenchRun {
  private Properties properties = new Properties();

  public void setProperties(Properties p) {
    properties = p;
  }

  public Properties getProperties() {
    return properties;
  }

  public void init() throws DBException {
  }

  public void cleanup() throws DBException {
  }

  public abstract Status query(String statement);
}
