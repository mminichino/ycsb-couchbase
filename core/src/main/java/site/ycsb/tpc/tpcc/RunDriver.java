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
  public static final String QUERIES_NUM_PROPERTY = "benchmark.queryNumber";
  public static final String QUERIES_NUM_PROPERTY_DEFAULT = "0";
  public static final String QUERIES_PRINT_PROPERTY = "benchmark.queryPrint";
  public static final String QUERIES_PRINT_PROPERTY_DEFAULT = "false";
  public static String queryClass;
  public static int queryNumber;
  public static boolean queryPrint;
  public static BenchQueries queries;

  @Override
  public void init(Properties p) throws WorkloadException {
    queryClass = p.getProperty(QUERIES_PROPERTY, QUERIES_PROPERTY_DEFAULT);
    queryNumber = Integer.parseInt(p.getProperty(QUERIES_NUM_PROPERTY, QUERIES_NUM_PROPERTY_DEFAULT));
    queryPrint = Boolean.parseBoolean(p.getProperty(QUERIES_PRINT_PROPERTY, QUERIES_PRINT_PROPERTY_DEFAULT));

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
    String[] queryList = queries.getQueryList();
    try {
      for (int i = 0; i < queryList.length; i++) {
        if (queryNumber > 0 && i != queryNumber - 1) {
          continue;
        }
        if (queryPrint) {
          System.out.println(queryList[i]);
          continue;
        }
        String query = queryList[i];
        List<ObjectNode> results = db.query(query);
        if (results == null) {
          LOGGER.warn("No results found for query: {}", query);
          continue;
        }
        System.out.printf("Query %d returned %d results\n", i + 1, results.size());
        LOGGER.info("Query #{}", i + 1);
        for (ObjectNode result : results) {
          LOGGER.info("\n{}", result.toPrettyString());
        }
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
