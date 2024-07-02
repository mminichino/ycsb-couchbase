package site.ycsb.tpc.tpcc;

import site.ycsb.*;

import java.util.List;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

public abstract class LoadDriver extends BenchLoad {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.tpcc.LoadDriver");

  public static final String TPCC_TRANSACTION_COUNT = "tpcc.transactionCount";
  public static final String TPCC_MAX_ITEMS = "tpcc.maxItems";
  public static final String TPCC_CUST_PER_DIST = "tpcc.custPerDist";
  public static final String TPCC_DIST_PER_WAREHOUSE = "tpcc.distPerWarehouse";
  public static final String TPCC_ORD_PER_DIST = "tpcc.ordPerDist";
  public static final String TPCC_MAX_NUM_ITEMS = "tpcc.maxNumItems";
  public static final String TPCC_MAX_ITEM_LEN = "tpcc.maxItemLen";

  public static TableKeys itemTable = new TableKeys().create("i_id", TableKeyType.INTEGER);
  public static TableKeys warehouseTable = new TableKeys().create("w_id", TableKeyType.INTEGER);
  public static TableKeys stockTable = new TableKeys().create("s_i_id", TableKeyType.INTEGER)
      .addForeignKey("s_w_id", TableKeyType.INTEGER);
  public static TableKeys districtTable = new TableKeys().create("d_id", TableKeyType.INTEGER)
      .addForeignKey("d_w_id", TableKeyType.INTEGER);
  public static TableKeys customerTable = new TableKeys().create("c_id", TableKeyType.INTEGER)
      .addForeignKey("c_d_id", TableKeyType.INTEGER)
      .addForeignKey("c_w_id", TableKeyType.INTEGER);
  public static TableKeys historyTable = new TableKeys().create("h_c_id", TableKeyType.INTEGER)
      .addForeignKey("h_c_d_id", TableKeyType.INTEGER)
      .addForeignKey("h_c_w_id", TableKeyType.INTEGER);
  public static TableKeys orderTable = new TableKeys().create("o_id", TableKeyType.INTEGER)
      .addForeignKey("o_d_id", TableKeyType.INTEGER)
      .addForeignKey("o_w_id", TableKeyType.INTEGER);
  public static TableKeys newOrderTable = new TableKeys().create("no_o_id", TableKeyType.INTEGER)
      .addForeignKey("no_d_id", TableKeyType.INTEGER)
      .addForeignKey("no_w_id", TableKeyType.INTEGER);
  public static TableKeys orderLineTable = new TableKeys().create("ol_o_id", TableKeyType.INTEGER)
      .addForeignKey("ol_d_id", TableKeyType.INTEGER)
      .addForeignKey("ol_w_id", TableKeyType.INTEGER)
      .addForeignKey("ol_number", TableKeyType.INTEGER);

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

  @Override
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

  public abstract Status createItemTable();
  public abstract Status createWarehouseTable();
  public abstract Status createStockTable();
  public abstract Status createDistrictTable();
  public abstract Status createCustomerTable();
  public abstract Status createHistoryTable();
  public abstract Status createOrderTable();
  public abstract Status createNewOrderTable();
  public abstract Status createOrderLineTable();

  public abstract void insertItemBatch(List<Item> batch);
  public abstract void insertWarehouseBatch(List<Warehouse> batch);
  public abstract void insertStockBatch(List<Stock> batch);
  public abstract void insertDistrictBatch(List<District> batch);
  public abstract void insertCustomerBatch(List<Customer> batch);
  public abstract void insertHistoryBatch(List<History> batch);
  public abstract void insertOrderBatch(List<Order> batch);
  public abstract void insertNewOrderBatch(List<NewOrder> batch);
  public abstract void insertOrderLineBatch(List<OrderLine> batch);

  @Override
  public void load() {
    createItemTable();
    createWarehouseTable();
    createStockTable();
    createDistrictTable();
    createCustomerTable();
    createHistoryTable();
    createOrderTable();
    createNewOrderTable();
    createOrderLineTable();

    LOGGER.info("Beginning data generation phase");

    insertItems();
    insertWarehouses();
    insertStock();
    insertDistrict();
    insertCustomer();
    insertHistory();
    insertOrders();
    insertNewOrders();
    insertOrderLines();
  }

  public Status insertItems() {
    LOGGER.info("Generating Item table data");
    try {
      generator.itemData().forEach(this::insertItemBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertWarehouses() {
    LOGGER.info("Generating Warehouses table data");
    try {
      generator.warehouseData().forEach(this::insertWarehouseBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertStock() {
    LOGGER.info("Generating Stock table data");
    try {
      generator.stockData().forEach(this::insertStockBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertDistrict() {
    LOGGER.info("Generating District table data");
    try {
      generator.districtData().forEach(this::insertDistrictBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertCustomer() {
    LOGGER.info("Generating Customer table data");
    try {
      generator.customerData().forEach(this::insertCustomerBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertHistory() {
    LOGGER.info("Generating History table data");
    try {
      generator.historyData().forEach(this::insertHistoryBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertOrders() {
    LOGGER.info("Generating Orders table data");
    try {
      generator.orderData().forEach(this::insertOrderBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertNewOrders() {
    LOGGER.info("Generating NewOrders table data");
    try {
      generator.newOrderData().forEach(this::insertNewOrderBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insertOrderLines() {
    LOGGER.info("Generating OrderLines table data");
    try {
      generator.orderLineData().forEach(this::insertOrderLineBatch);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
    return Status.OK;
  }
}