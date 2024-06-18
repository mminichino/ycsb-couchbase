package site.ycsb.workloads;

import site.ycsb.SQLDB;
import site.ycsb.SQLWorkload;
import site.ycsb.WorkloadException;
import site.ycsb.tpc.TPCCLoad;

import java.util.Properties;

public class TPCC extends SQLWorkload {
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
  private static boolean enableDebug = false;

  @Override
  public void init(Properties p) throws WorkloadException {
    transactionCount = Integer.parseInt(p.getProperty("tpcc.transactionCount", "5"));
    maxItems = Integer.parseInt(p.getProperty("tpcc.maxItems", "100000"));
    custPerDist = Integer.parseInt(p.getProperty("tpcc.custPerDist", "3000"));
    distPerWarehouse = Integer.parseInt(p.getProperty("tpcc.distPerWarehouse", "10"));
    ordPerDist = Integer.parseInt(p.getProperty("tpcc.ordPerDist", "3000"));
    maxNumItems = Integer.parseInt(p.getProperty("tpcc.maxNumItems", "15"));
    maxItemLen = Integer.parseInt(p.getProperty("tpcc.maxItemLen", "24"));
  }

  @Override
  public boolean load(SQLDB db, Object threadState) {
    TPCCLoad.TPCCLoadBuilder builder = new TPCCLoad.TPCCLoadBuilder();
    TPCCLoad tpcc = builder
        .custPerDist(custPerDist)
        .distPerWarehouse(distPerWarehouse)
        .ordPerDist(ordPerDist)
        .maxItems(maxItems)
        .enableDebug(enableDebug)
        .build();
    return false;
  }

  @Override
  public boolean run(SQLDB db, Object threadState) {
    return false;
  }
}
