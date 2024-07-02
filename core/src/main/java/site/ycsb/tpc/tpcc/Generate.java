package site.ycsb.tpc.tpcc;

import site.ycsb.Batch;
import site.ycsb.tpc.TPCCUtil;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class Generate {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.tpcc.Generate");
  private final int maxItems;
  private final int custPerDist;
  private final int distPerWarehouse;
  private final int ordPerDist;
  private final int warehouseCount;
  private final int batchSize;
  private final boolean enableDebug;
  private final TPCCUtil util;

  public List<Item> item = new ArrayList<>();
  public List<Warehouse> warehouse = new ArrayList<>();
  public List<Stock> stock = new ArrayList<>();
  public List<District> district = new ArrayList<>();
  public List<Customer> customer = new ArrayList<>();
  public List<History> history = new ArrayList<>();
  public List<Order> orders = new ArrayList<>();
  public List<NewOrder> newOrders = new ArrayList<>();
  public List<OrderLine> orderLine = new ArrayList<>();

  public static class GeneratorBuilder {
    private int maxItems = 100000;
    private int custPerDist = 3000;
    private int distPerWarehouse = 10;
    private int ordPerDist = 3000;
    private int warehouseCount = 1;
    private int batchSize = 1000;
    private boolean enableDebug = false;

    public GeneratorBuilder maxItems(int value) {
      this.maxItems = value;
      return this;
    }

    public GeneratorBuilder custPerDist(int value) {
      this.custPerDist = value;
      return this;
    }

    public GeneratorBuilder distPerWarehouse(int value) {
      this.distPerWarehouse = value;
      return this;
    }

    public GeneratorBuilder ordPerDist(int value) {
      this.ordPerDist = value;
      return this;
    }

    public GeneratorBuilder warehouseCount(int value) {
      this.warehouseCount = value;
      return this;
    }

    public GeneratorBuilder batchSize(int value) {
      this.batchSize = value;
      return this;
    }

    public GeneratorBuilder enableDebug(boolean value) {
      this.enableDebug = value;
      return this;
    }

    public Generate build() {
      return new Generate(this);
    }
  }

  private Generate(GeneratorBuilder builder) {
    this.maxItems = builder.maxItems;
    this.custPerDist = builder.custPerDist;
    this.distPerWarehouse = builder.distPerWarehouse;
    this.ordPerDist = builder.ordPerDist;
    this.warehouseCount = builder.warehouseCount;
    this.batchSize = builder.batchSize;
    this.enableDebug = builder.enableDebug;
    this.util = new TPCCUtil(custPerDist, ordPerDist);
    this.util.initPermutation();
  }

  public void createSchema() {
    generateItems();
    for (int warehouse = 0; warehouse < warehouseCount; warehouse++) {
      generateWarehouse(warehouse);
      generateDistrict(warehouse);
    }
  }

  public Stream<List<Item>> itemData() {
    return Batch.split(item, batchSize);
  }

  public Stream<List<Warehouse>> warehouseData() {
    return Batch.split(warehouse, batchSize);
  }

  public Stream<List<Stock>> stockData() {
    return Batch.split(stock, batchSize);
  }

  public Stream<List<District>> districtData() {
    return Batch.split(district, batchSize);
  }

  public Stream<List<Customer>> customerData() {
    return Batch.split(customer, batchSize);
  }

  public Stream<List<History>> historyData() {
    return Batch.split(history, batchSize);
  }

  public Stream<List<Order>> orderData() {
    return Batch.split(orders, batchSize);
  }

  public Stream<List<NewOrder>> newOrderData() {
    return Batch.split(newOrders, batchSize);
  }

  public Stream<List<OrderLine>> orderLineData() {
    return Batch.split(orderLine, batchSize);
  }

  public void generateItems() {
    int[] orig = new int[maxItems + 1];
    int pos;
    int i;

    LOGGER.debug("begin item table data generation");

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }
    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = util.randomNumber(0, maxItems);
      } while (orig[pos] != 0);
      orig[pos] = 1;
    }

    for (int i_id = 1; i_id <= maxItems; i_id++) {
      item.add(new Item(i_id, util, orig));
    }

    LOGGER.debug("item table data generation complete");
  }

  public void generateWarehouse(int warehouseNum) {
    LOGGER.debug("begin warehouse {} data generation", warehouseNum);

    warehouse.add(new Warehouse(warehouseNum, util));
    generateStock(warehouseNum);
    generateDistrict(warehouseNum);

    LOGGER.debug("warehouse {} table data generation complete", warehouseNum);
  }

  public void generateStock(int warehouseNum) {
    int[] orig = new int[maxItems + 1];
    int pos;
    int i;

    LOGGER.debug("begin stock data generation for warehouse {}", warehouseNum);

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }

    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = util.randomNumber(0, maxItems);
      } while (orig[pos] != 0);
      orig[pos] = 1;
    }

    for (int s_i_id = 1; s_i_id <= maxItems; s_i_id++) {
      stock.add(new Stock(s_i_id, warehouseNum, util, orig));
    }

    LOGGER.debug("stock table data generation complete for warehouse {}", warehouseNum);
  }

  public void generateDistrict(int warehouseNum) {
    LOGGER.debug("begin district data generation for warehouse {}", warehouseNum);

    for (int d_id = 1; d_id <= distPerWarehouse; d_id++) {
      district.add(new District(d_id, warehouseNum, util));
      generateCustomers(warehouseNum, d_id);
      generateOrders(warehouseNum, d_id);
    }

    LOGGER.debug("district table data generation complete for warehouse {}", warehouseNum);
  }

  public void generateCustomers(int warehouseNum, int districtNum) {
    LOGGER.debug("begin customer data generation for warehouse {}", warehouseNum);

    for (int c_id = 1; c_id <= custPerDist; c_id++) {
      customer.add(new Customer(c_id, districtNum, warehouseNum, util));
      history.add(new History(c_id, districtNum, warehouseNum, util));
    }

    LOGGER.debug("customer table data generation complete for warehouse {}", warehouseNum);
  }

  public void generateOrders(int warehouseNum, int districtNum) {
    LOGGER.debug("begin order data generation for warehouse {}", warehouseNum);

    for (int o_id = 1; o_id <= ordPerDist; o_id++) {
      int o_ol_cnt = util.randomNumber(5, 15);
      orders.add(new Order(o_id, districtNum, warehouseNum, util));
      if (o_id > 2100) {
        newOrders.add(new NewOrder(o_id, districtNum, warehouseNum));
      }
      for (int ol = 1; ol <= o_ol_cnt; ol++) {
        orderLine.add(new OrderLine(ol, o_id, districtNum, warehouseNum, maxItems, util));
      }
    }

    LOGGER.debug("order table data generation complete for warehouse {}", warehouseNum);
  }
}
