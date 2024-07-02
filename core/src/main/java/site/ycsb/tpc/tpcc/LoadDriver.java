package site.ycsb.tpc.tpcc;

import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.List;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

public abstract class LoadDriver {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.tpcc.LoadDriver");

  public static final String TPCC_TRANSACTION_COUNT = "tpcc.transactionCount";
  public static final String TPCC_MAX_ITEMS = "tpcc.maxItems";
  public static final String TPCC_CUST_PER_DIST = "tpcc.custPerDist";
  public static final String TPCC_DIST_PER_WAREHOUSE = "tpcc.distPerWarehouse";
  public static final String TPCC_ORD_PER_DIST = "tpcc.ordPerDist";
  public static final String TPCC_MAX_NUM_ITEMS = "tpcc.maxNumItems";
  public static final String TPCC_MAX_ITEM_LEN = "tpcc.maxItemLen";

  private static int transactionCount;
  private static int maxItems;
  private static int custPerDist;
  private static int distPerWarehouse;
  private static int ordPerDist;
  private static int maxNumItems;
  private static int maxItemLen;
  private static boolean enableDebug;
  private static int warehouseCount;

  private Properties properties = new Properties();
  private Generate generator;

  public void setProperties(Properties p) {
    properties = p;

    transactionCount = Integer.parseInt(p.getProperty("tpcc.transactionCount", "5"));
    maxItems = Integer.parseInt(p.getProperty("tpcc.maxItems", "100000"));
    custPerDist = Integer.parseInt(p.getProperty("tpcc.custPerDist", "3000"));
    distPerWarehouse = Integer.parseInt(p.getProperty("tpcc.distPerWarehouse", "10"));
    ordPerDist = Integer.parseInt(p.getProperty("tpcc.ordPerDist", "3000"));
    maxNumItems = Integer.parseInt(p.getProperty("tpcc.maxNumItems", "15"));
    maxItemLen = Integer.parseInt(p.getProperty("tpcc.maxItemLen", "24"));
    warehouseCount = Integer.parseInt(p.getProperty("tpcc.warehouseCount", "1"));
  }

  public Properties getProperties() {
    return properties;
  }

  public void init() throws DBException {
  }

  public void cleanup() throws DBException {
  }

  public void generate() {
    Generate.GeneratorBuilder builder = new Generate.GeneratorBuilder();
    generator = builder
        .custPerDist(custPerDist)
        .distPerWarehouse(distPerWarehouse)
        .ordPerDist(ordPerDist)
        .warehouseCount(warehouseCount)
        .maxItems(maxItems)
        .enableDebug(enableDebug)
        .build();
    generator.createSchema();
  }

  public abstract void insertBatch(List<?> batch);

  public Status insertItems() {
    try {
      generator.itemData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertWarehouses() {
    try {
      generator.warehouseData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertStock() {
    try {
      generator.stockData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertDistrict() {
    try {
      generator.districtData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertCustomer() {
    try {
      generator.customerData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertHistory() {
    try {
      generator.historyData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertOrders() {
    try {
      generator.orderData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertNewOrders() {
    try {
      generator.newOrderData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertOrderLines() {
    try {
      generator.orderLineData().forEach(this::insertBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }
}
