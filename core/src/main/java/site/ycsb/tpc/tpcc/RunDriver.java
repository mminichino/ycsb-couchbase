package site.ycsb.tpc.tpcc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.*;

import java.util.List;
import java.util.Properties;

public class RunDriver extends BenchWorkload {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.tpcc.RunDriver");
  public static final String QUERIES_PROPERTY = "benchmark.queryClass";
  public static final String QUERIES_PROPERTY_DEFAULT = "site.ycsb.tpc.tpcc.SQLQueries";
  public static String queryClass;
  public static BenchQueries queries;

  @Override
  public void init(Properties p) throws WorkloadException {
    queryClass = p.getProperty(QUERIES_PROPERTY, QUERIES_PROPERTY_DEFAULT);

    try {
      ClassLoader classLoader = RunDriver.class.getClassLoader();
      Class<?> queryClassLoader = classLoader.loadClass(queryClass);
      queries = (BenchQueries) queryClassLoader.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new WorkloadException(e);
    }
  }

  @Override
  public boolean test(BenchRun db, Object threadState) {
    try {
      for (String query : queries.getQueryList()) {
        List<ObjectNode> results = db.query(query);
        if (results == null) {
          return false;
        }
        for (ObjectNode result : results) {
          System.out.println(result.toPrettyString());
        }
        return true;
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
    return false;
  }

  @Override
  public boolean run(BenchRun db, Object threadState) {
    return false;
  }
}
