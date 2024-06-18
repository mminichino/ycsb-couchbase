package site.ycsb.tpc;

import site.ycsb.SQLDB;
import site.ycsb.Record;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class TPCCLoad {
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

  public static void init(Properties properties) {
    transactionCount = Integer.parseInt(properties.getProperty("tpcc.transactionCount", "5"));
    maxItems = Integer.parseInt(properties.getProperty("tpcc.maxItems", "100000"));
    custPerDist = Integer.parseInt(properties.getProperty("tpcc.custPerDist", "3000"));
    distPerWarehouse = Integer.parseInt(properties.getProperty("tpcc.distPerWarehouse", "10"));
    ordPerDist = Integer.parseInt(properties.getProperty("tpcc.ordPerDist", "3000"));
    maxNumItems = Integer.parseInt(properties.getProperty("tpcc.maxNumItems", "15"));
    maxItemLen = Integer.parseInt(properties.getProperty("tpcc.maxItemLen", "24"));
  }

  public static void loadItems(SQLDB db) {
    int i_id = 0;
    int i_im_id = 0;
    String i_name = null;
    float i_price = 0;
    String i_data = null;

    int[] orig = new int[maxItems + 1];
    int pos = 0;
    int i = 0;

    System.out.println("Loading Item");

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }
    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = TPCCUtil.randomNumber(0, maxItems);
      } while (orig[pos] != 0);
      orig[pos] = 1;
    }

    db.createTable("item", Tables.itemTableC, Tables.itemKeysC);

    for (i_id = 1; i_id <= maxItems; i_id++) {
      i_im_id = TPCCUtil.randomNumber(1, 10000);

      i_name = TPCCUtil.makeAlphaString(14, 24);

      i_price = (float) (TPCCUtil.randomNumber(100, 10000) / 100.0);

      i_data = TPCCUtil.makeAlphaString(26, 50);
      if (orig[i_id] != 0) {
        pos = TPCCUtil.randomNumber(0, i_data.length() - 8);
        i_data = i_data.substring(0, pos) + "original" + i_data.substring(pos + 8);
      }

      Record record = new Record();
      record.add("i_id", i_id);
      record.add("i_im_id", i_im_id);
      record.add("i_name", i_name);
      record.add("i_price", i_price);
      record.add("i_data", i_data);
      db.insert("item", record);
    }

    System.out.println("Item Done");
  }

  public static void loadWare(SQLDB db, int min_ware, int max_ware) {
    int w_id;
    String w_name = null;
    String w_street_1 = null;
    String w_street_2 = null;
    String w_city = null;
    String w_state = null;
    String w_zip = null;
    double w_tax = 0;
    double w_ytd = 0;

    int tmp = 0;
    int currentShard = 0;

    System.out.println("Loading Warehouse");

    db.createTable("warehouse", Tables.warehouseTableC, Tables.warehouseKeysC);

    for (w_id = min_ware; w_id <= max_ware; w_id++) {
      System.out.println("Current Shard: " + currentShard);

      w_name = TPCCUtil.makeAlphaString(6, 10);
      w_street_1 = TPCCUtil.makeAlphaString(10, 20);
      w_street_2 = TPCCUtil.makeAlphaString(10, 20);
      w_city = TPCCUtil.makeAlphaString(10, 20);
      w_state = TPCCUtil.makeAlphaString(2, 2);
      w_zip = TPCCUtil.makeAlphaString(9, 9);

      w_tax = ((double) TPCCUtil.randomNumber(10, 20) / 100.0);
      w_ytd = 3000000.00;

      if (enableDebug) {
        System.out.printf("WID = %d, Name = %s, Tax = %f\n", w_id, w_name, w_tax);
      }

      Record record = new Record();
      record.add("w_id", w_id);
      record.add("w_name", w_name);
      record.add("w_street_1", w_street_1);
      record.add("w_street_2", w_street_2);
      record.add("w_city", w_city);
      record.add("w_state", w_state);
      record.add("w_zip", w_zip);
      record.add("w_tax", w_tax);
      record.add("w_ytd", w_ytd);
      db.insert("warehouse", record);

      stock(db, w_id);
      district(db, w_id);
    }
  }

  public static void loadCust(SQLDB db, int min_ware, int max_ware) {
    try {
      for (int w_id = min_ware; w_id <= max_ware; w_id++) {
        for (int d_id = 1; d_id <= distPerWarehouse; d_id++) {
          loadCustomer(db, d_id, w_id);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error loading customers", e);
    }
  }

  public static void loadOrd(SQLDB db, int max_ware) {
    try {
      for (int w_id = 1; w_id <= max_ware; w_id++) {
        for (int d_id = 1; d_id <= distPerWarehouse; d_id++) {
          loadOrders(db, d_id, w_id);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to load orders", e);
    }
  }

  public static void stock(SQLDB db, int w_id) {
    int s_i_id = 0;
    int s_w_id = 0;
    int s_quantity = 0;

    String s_dist_01 = null;
    String s_dist_02 = null;
    String s_dist_03 = null;
    String s_dist_04 = null;
    String s_dist_05 = null;
    String s_dist_06 = null;
    String s_dist_07 = null;
    String s_dist_08 = null;
    String s_dist_09 = null;
    String s_dist_10 = null;
    String s_data = null;

    int[] orig = new int[maxItems + 1];
    int pos = 0;
    int i = 0;
    boolean error = false;

    db.createTable("stock", Tables.stockTableC, Tables.stockKeysC);

    System.out.printf("Loading Stock Wid=%d\n", w_id);
    s_w_id = w_id;

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }

    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = TPCCUtil.randomNumber(0, maxItems);
      } while (orig[pos] != 0); //TODO: FIx later
      orig[pos] = 1;
    }

    for (s_i_id = 1; s_i_id <= maxItems; s_i_id++) {
      s_quantity = TPCCUtil.randomNumber(10, 100);

      s_dist_01 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_02 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_03 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_04 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_05 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_06 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_07 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_08 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_09 = TPCCUtil.makeAlphaString(24, 24);
      s_dist_10 = TPCCUtil.makeAlphaString(24, 24);

      s_data = TPCCUtil.makeAlphaString(26, 50);

      if (orig[s_i_id] != 0) {
        s_data = "original";
      }

      Record record = new Record();
      record.add("s_i_id", s_i_id);
      record.add("s_w_id", s_w_id);
      record.add("s_quantity", s_quantity);
      record.add("s_dist_01", s_dist_01);
      record.add("s_dist_02", s_dist_02);
      record.add("s_dist_03", s_dist_03);
      record.add("s_dist_04", s_dist_04);
      record.add("s_dist_05", s_dist_05);
      record.add("s_dist_06", s_dist_06);
      record.add("s_dist_07", s_dist_07);
      record.add("s_dist_08", s_dist_08);
      record.add("s_dist_09", s_dist_09);
      record.add("s_dist_10", s_dist_10);
      record.add("s_ytd", 0);
      record.add("s_order_cnt", 0);
      record.add("s_remote_cnt", 0);
      record.add("s_data", s_data);
      db.insert("stock", record);

      if (enableDebug)
        System.out.printf("SID = %d, WID = %d, Quan = %d\n", s_i_id, s_w_id, s_quantity);
    }

    System.out.println("Stock Done");
  }

  public static void district(SQLDB db, int w_id) {
    int d_id;
    int d_w_id;
    String d_name;
    String d_street_1;
    String d_street_2;
    String d_city;
    String d_state;
    String d_zip;
    float d_tax;
    float d_ytd;
    int d_next_o_id;
    boolean error = false;

    System.out.println("Loading District");

    d_w_id = w_id;
    d_ytd = (float) 30000.0;
    d_next_o_id = 3001;

    db.createTable("district", Tables.districtTableC, Tables.districtKeysC);

    for (d_id = 1; d_id <= distPerWarehouse; d_id++) {
      d_name = TPCCUtil.makeAlphaString(6, 10);
      d_street_1 = TPCCUtil.makeAlphaString(10, 20);
      d_street_2 = TPCCUtil.makeAlphaString(10, 20);
      d_city = TPCCUtil.makeAlphaString(10, 20);
      d_state = TPCCUtil.makeAlphaString(2, 2);
      d_zip = TPCCUtil.makeAlphaString(9, 9);

      d_tax = (float) (((float) TPCCUtil.randomNumber(10, 20)) / 100.0);

      Record record = new Record();
      record.add("d_id", d_id);
      record.add("d_w_id", d_w_id);
      record.add("d_name", d_name);
      record.add("d_street_1", d_street_1);
      record.add("d_street_2", d_street_2);
      record.add("d_city", d_city);
      record.add("d_state", d_state);
      record.add("d_zip", d_zip);
      record.add("d_tax", d_tax);
      record.add("d_ytd", d_ytd);
      record.add("d_next_o_id", d_next_o_id);
      db.insert("district", record);

      if (enableDebug)
        System.out.printf("DID = %d, WID = %d, Name = %s, Tax = %f\n",
            d_id, d_w_id, d_name, d_tax);
    }
  }

  public static void loadCustomer(SQLDB db, int d_id, int w_id) {
    int c_id = 0;
    int c_d_id = 0;
    int c_w_id = 0;
    String c_first = null;
    String c_middle = null;
    String c_last = null;
    String c_street_1 = null;
    String c_street_2 = null;
    String c_city = null;
    String c_state = null;
    String c_zip = null;
    String c_phone = null;
    String c_credit = null;
    int c_credit_lim = 0;
    float c_discount = 0;
    float c_balance = 0;
    String c_data = null;
    double h_amount = 0.0;
    String h_data = null;

    System.out.printf("Loading Customer for DID=%d, WID=%d\n", d_id, w_id);

    db.createTable("customer", Tables.customerTableC, Tables.customerKeysC);
    db.createTable("history", Tables.historyTableC, Tables.historyKeysC);

    for (c_id = 1; c_id <= custPerDist; c_id++) {
      c_d_id = d_id;
      c_w_id = w_id;

      c_first = TPCCUtil.makeAlphaString(8, 16);
      c_middle = "O" + "E";

      if (c_id <= 1000) {
        c_last = TPCCUtil.lastName(c_id - 1);
      } else {
        c_last = TPCCUtil.lastName(TPCCUtil.nuRand(255, 0, 999));
      }

      c_street_1 = TPCCUtil.makeAlphaString(10, 20);
      c_street_2 = TPCCUtil.makeAlphaString(10, 20);
      c_city = TPCCUtil.makeAlphaString(10, 20);
      c_state = TPCCUtil.makeAlphaString(2, 2);
      c_zip = TPCCUtil.makeAlphaString(9, 9);

      c_phone = TPCCUtil.makeNumberString(16, 16);

      if (TPCCUtil.randomNumber(0, 1) == 1)
        c_credit = "G";
      else
        c_credit = "B";
      c_credit += "C";

      c_credit_lim = 50000;
      c_discount = (float) (((float) TPCCUtil.randomNumber(0, 50)) / 100.0);
      c_balance = (float) -10.0;

      c_data = TPCCUtil.makeAlphaString(300, 500);

      String dateFormat = "yy-MM-dd'T'HH:mm:ss";
      SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
      String date = timeStampFormat.format(new Date());

      try {
        Record record = new Record();
        record.add("c_id", c_id);
        record.add("c_d_id", c_d_id);
        record.add("c_w_id", c_w_id);
        record.add("c_first", c_first);
        record.add("c_middle", c_middle);
        record.add("c_last", c_last);
        record.add("c_street_1", c_street_1);
        record.add("c_street_2", c_street_2);
        record.add("c_city", c_city);
        record.add("c_state", c_state);
        record.add("c_zip", c_zip);
        record.add("c_phone", c_phone);
        record.add("date", date);
        record.add("c_credit", c_credit);
        record.add("c_credit_lim", c_credit_lim);
        record.add("c_discount", c_discount);
        record.add("c_balance", c_balance);
        record.add("c_ytd_payment", 10.0);
        record.add("c_payment_cnt", 1);
        record.add("c_delivery_cnt", 0);
        record.add("c_data", c_data);
        db.insert("customer", record);
      } catch (Exception e) {
        throw new RuntimeException("Customer insert error", e);
      }

      h_amount = 10.0;
      h_data = TPCCUtil.makeAlphaString(12, 24);

      try {
        Record record = new Record();
        record.add("h_c_id", c_id);
        record.add("h_c_d_id", c_d_id);
        record.add("h_c_w_id", c_w_id);
        record.add("h_d_id", c_d_id);
        record.add("h_w_id", c_w_id);
        record.add("h_date", date);
        record.add("h_amount", h_amount);
        record.add("h_data", h_data);
        db.insert("history", record);
      } catch (Exception e) {
        throw new RuntimeException("Insert into History error", e);
      }
      if (enableDebug) {
        System.out.printf("CID = %d, LST = %s, P# = %s\n",
            c_id, c_last, c_phone);
      }
    }
    System.out.println("Customer Done");
  }

  public static void loadOrders(SQLDB db, int d_id, int w_id) {
    int o_id;
    int o_c_id;
    int o_d_id;
    int o_w_id;
    int o_carrier_id;
    int o_ol_cnt;
    int ol;
    int ol_i_id;
    int ol_supply_w_id;
    int ol_quantity;
    float ol_amount;
    String ol_dist_info;
    float tmp_float;

    db.createTable("orders", Tables.orderTableC, Tables.orderKeysC);
    db.createTable("new_orders", Tables.newOrderTableC, Tables.newOrderKeysC);
    db.createTable("order_line", Tables.orderLineTableC, Tables.orderLineKeysC);

    System.out.printf("Loading Orders for D=%d, W=%d\n", d_id, w_id);
    o_d_id = d_id;
    o_w_id = w_id;
    TPCCUtil.initPermutation(custPerDist, ordPerDist);
    for (o_id = 1; o_id <= ordPerDist; o_id++) {
      o_c_id = TPCCUtil.getPermutation(ordPerDist);
      o_carrier_id = TPCCUtil.randomNumber(1, 10);
      o_ol_cnt = TPCCUtil.randomNumber(5, 15);

      String dateFormat = "yy-MM-dd'T'HH:mm:ss";
      SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
      String date = timeStampFormat.format(new Date());

      if (o_id > 2100) {
        Record record = new Record();
        record.add("o_id", o_id);
        record.add("o_d_id", o_d_id);
        record.add("o_w_id", o_w_id);
        record.add("o_c_id", o_c_id);
        record.add("o_entry_d", date);
        record.add("o_carrier_id");
        record.add("o_ol_cnt", o_ol_cnt);
        record.add("o_all_local", 1);
        db.insert("orders", record);

        record = new Record();
        record.add("no_o_id", o_id);
        record.add("no_d_id", o_d_id);
        record.add("no_w_id", o_w_id);
        db.insert("new_orders", record);
      } else {
        Record record = new Record();
        record.add("o_id", o_id);
        record.add("o_d_id", o_d_id);
        record.add("o_w_id", o_w_id);
        record.add("o_c_id", o_c_id);
        record.add("o_entry_d", date);
        record.add("o_carrier_id", o_carrier_id);
        record.add("o_ol_cnt", o_ol_cnt);
        record.add("o_all_local", 1);
        db.insert("orders", record);
      }

      if (enableDebug)
        System.out.printf("OID = %d, CID = %d, DID = %d, WID = %d\n",
            o_id, o_c_id, o_d_id, o_w_id);

      for (ol = 1; ol <= o_ol_cnt; ol++) {
        ol_i_id = TPCCUtil.randomNumber(1, maxItems);
        ol_supply_w_id = o_w_id;
        ol_quantity = 5;
        ol_amount = (float) 0.0;

        ol_dist_info = TPCCUtil.makeAlphaString(24, 24);

        tmp_float = (float) ((float) (TPCCUtil.randomNumber(10, 10000)) / 100.0);

        if (o_id > 2100) {
          Record record = new Record();
          record.add("ol_o_id", o_id);
          record.add("ol_d_id", o_d_id);
          record.add("ol_w_id", o_w_id);
          record.add("ol_number", ol);
          record.add("ol_i_id", ol_i_id);
          record.add("ol_supply_w_id", ol_supply_w_id);
          record.add("ol_delivery_d");
          record.add("ol_quantity", ol_quantity);
          record.add("ol_amount", ol_amount);
          record.add("ol_dist_info", ol_dist_info);
          db.insert("order_line", record);
        } else {
          Record record = new Record();
          record.add("ol_o_id", o_id);
          record.add("ol_d_id", o_d_id);
          record.add("ol_w_id", o_w_id);
          record.add("ol_number", ol);
          record.add("ol_i_id", ol_i_id);
          record.add("ol_supply_w_id", ol_supply_w_id);
          record.add("ol_delivery_d", date);
          record.add("ol_quantity", ol_quantity);
          record.add("ol_amount", tmp_float);
          record.add("ol_dist_info", ol_dist_info);
          db.insert("order_line", record);
        }

        if (enableDebug) {
          System.out.printf("OL = %d, IID = %d, QUAN = %d, AMT = %f\n",
              ol, ol_i_id, ol_quantity, ol_amount);
        }
      }
    }

    System.out.println("Orders Done");
  }
}
