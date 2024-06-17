package site.ycsb;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;
import java.util.Set;
import java.util.Map;

/**
 * SQL Database Driver
 */
public abstract class SQLDB {
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

  public abstract Status createTable(String table, Map<String, DataType> columns, Set<String> keys);

  public abstract Status dropTable(String table, Map<String, DataType> columns, Set<String> keys);

  public abstract Status select(String table, String statement);

  public abstract Status insert(String table, Record data);

  public abstract Status update(String table, Record data);

  public abstract Status query(String statement);

  public abstract Status delete(String table, String statement);
}
