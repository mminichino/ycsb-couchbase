package site.ycsb;

import com.google.gson.JsonObject;

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

  public abstract Status select(String table, Set<String> fields, String from, String where);

  public abstract Status insert(String table, Set<String> fields);

  public abstract Status update(String table, Map<String, String> fields, String where);

  public abstract Status query(String sql);

}
