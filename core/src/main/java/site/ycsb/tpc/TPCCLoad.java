package site.ycsb.tpc;

import site.ycsb.SQLDB;
import site.ycsb.Record;

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

  public static void loadCust(TpccLoadConfig loadConfig, int shardCount, int min_ware, int max_ware, int shardId) {
    try {
      for (int w_id = min_ware; w_id <= max_ware; w_id++) {
        for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
          loadCustomer(loadConfig, d_id, w_id, shardCount, shardId);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error loading customers", e);
    }
  }

  /*
   * ==================================================================+ |
   * ROUTINE NAME |      LoadOrd | DESCRIPTION |      Loads the Orders and
   * Order_Line Tables | ARGUMENTS |      none
   * +==================================================================
   */
  public static void loadOrd(TpccLoadConfig loadConfig, int shardCount, int max_ware, int shardId) {
    try {
      // for each warehouse
      for (int w_id = 1; w_id <= max_ware; w_id++) {
        // for each district
        for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
          // generate orders
          loadOrders(loadConfig, d_id, w_id, shardCount, shardId);
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

    //int sdatasize = 0;
    int[] orig = new int[MAXITEMS + 1];
    int pos = 0;
    int i = 0;
    boolean error = false;

    final String STOCK_COLUMN_NAME[] = {"s_i_id", " s_w_id", " s_quantity", " " +
        "s_dist_01", " s_dist_02", " s_dist_03", " s_dist_04", " s_dist_05", " s_dist_06", " " +
        "s_dist_07", " s_dist_08", " s_dist_09", " s_dist_10", " s_ytd", " s_order_cnt", " " +
        "s_remote_cnt", " s_data"
    };

    final Record stockRecord = new Record(17);
    RecordLoader stockLoader = loadConfig.createLoader("stock", STOCK_COLUMN_NAME);

    /* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/
    System.out.printf("Loading Stock Wid=%d\n", w_id);
    s_w_id = w_id;

    for (i = 0; i < MAXITEMS / 10; i++) {
      orig[i] = 0;
    }

    for (i = 0; i < MAXITEMS / 10; i++) {
      do {
        pos = Util.randomNumber(0, MAXITEMS);
      } while (orig[pos] != 0); //TODO: FIx later
      orig[pos] = 1;
    }

    ////retry:
    for (s_i_id = 1; s_i_id <= MAXITEMS; s_i_id++) {

      /* Generate Stock Data */
      s_quantity = Util.randomNumber(10, 100);

      s_dist_01 = Util.makeAlphaString(24, 24);
      s_dist_02 = Util.makeAlphaString(24, 24);
      s_dist_03 = Util.makeAlphaString(24, 24);
      s_dist_04 = Util.makeAlphaString(24, 24);
      s_dist_05 = Util.makeAlphaString(24, 24);
      s_dist_06 = Util.makeAlphaString(24, 24);
      s_dist_07 = Util.makeAlphaString(24, 24);
      s_dist_08 = Util.makeAlphaString(24, 24);
      s_dist_09 = Util.makeAlphaString(24, 24);
      s_dist_10 = Util.makeAlphaString(24, 24);


      s_data = Util.makeAlphaString(26, 50);
      //sdatasize = s_data.length();
      if (orig[s_i_id] != 0) {//TODO:Change this later
        //pos = Util.randomNumber(0, sdatasize - 8);
        s_data = "original";
      }
            /*EXEC SQL INSERT INTO
                               stock
                               values(:s_i_id,:s_w_id,:s_quantity,
                              :s_dist_01,:s_dist_02,:s_dist_03,:s_dist_04,:s_dist_05,
                              :s_dist_06,:s_dist_07,:s_dist_08,:s_dist_09,:s_dist_10,
                              0, 0, 0,:s_data);*/

      stockRecord.reset();
      stockRecord.add(s_i_id);
      stockRecord.add(s_w_id);
      stockRecord.add(s_quantity);
      stockRecord.add(s_dist_01);
      stockRecord.add(s_dist_02);
      stockRecord.add(s_dist_03);
      stockRecord.add(s_dist_04);
      stockRecord.add(s_dist_05);
      stockRecord.add(s_dist_06);
      stockRecord.add(s_dist_07);
      stockRecord.add(s_dist_08);
      stockRecord.add(s_dist_09);
      stockRecord.add(s_dist_10);
      stockRecord.add(0);
      stockRecord.add(0);
      stockRecord.add(0);
      stockRecord.add(s_data);

      stockLoader.load(stockRecord);

      if (optionDebug)
        System.out.printf("SID = %d, WID = %d, Quan = %d\n",
            s_i_id, s_w_id, s_quantity);

      if ((s_i_id % 100) == 0) {
        System.out.printf(".");
        if ((s_i_id % 5000) == 0)
          System.out.printf(" %d\n", s_i_id);
      }
    }

    stockLoader.close();

    System.out.printf(" Stock Done.\n");
    return error;

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

    System.out.printf("Loading District\n");
    d_w_id = w_id;
    d_ytd = (float) 30000.0;
    d_next_o_id = 3001;

    final String[] DISTRICT_COLUMN_NAME = {"d_id", " d_w_id", " d_name", " d_street_1", " d_street_2", " d_city", " d_state", " d_zip", " d_tax", " d_ytd", " d_next_o_id"};
    final Record districtRecord = new Record(11);
    final RecordLoader districtLoader = loadConfig.createLoader("district", DISTRICT_COLUMN_NAME);

    //retry:
    for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {

      /* Generate District Data */

      d_name = Util.makeAlphaString(6, 10);
      d_street_1 = Util.makeAlphaString(10, 20);
      d_street_2 = Util.makeAlphaString(10, 20);
      d_city = Util.makeAlphaString(10, 20);
      d_state = Util.makeAlphaString(2, 2);
      d_zip = Util.makeAlphaString(9, 9);

      d_tax = (float) (((float) Util.randomNumber(10, 20)) / 100.0);

            /*EXEC SQL INSERT INTO
                               district
                               values(:d_id,:d_w_id,:d_name,
                              :d_street_1,:d_street_2,:d_city,:d_state,:d_zip,
                              :d_tax,:d_ytd,:d_next_o_id);*/

      districtRecord.reset();
      districtRecord.add(d_id);
      districtRecord.add(d_w_id);
      districtRecord.add(d_name);
      districtRecord.add(d_street_1);
      districtRecord.add(d_street_2);
      districtRecord.add(d_city);
      districtRecord.add(d_state);
      districtRecord.add(d_zip);
      districtRecord.add(d_tax);
      districtRecord.add(d_ytd);
      districtRecord.add(d_next_o_id);

      districtLoader.load(districtRecord);

      if (optionDebug)
        System.out.printf("DID = %d, WID = %d, Name = %s, Tax = %f\n",
            d_id, d_w_id, d_name, d_tax);

    }

    districtLoader.close();

  }

  /*
   * ==================================================================+ |
   * ROUTINE NAME |      Customer | DESCRIPTION |      Loads Customer Table |
   * Also inserts corresponding history record | ARGUMENTS |      id   -
   * customer id |      d_id - district id |      w_id - warehouse id
   * +==================================================================
   */
  public static void loadCustomer(TpccLoadConfig loadConfig, int d_id, int w_id, int shardCount, int shardId) throws Exception {
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
//        String c_since = null;
    String c_credit = null;

    int c_credit_lim = 0;
    float c_discount = 0;
    float c_balance = 0;
    String c_data = null;

    double h_amount = 0.0;

    String h_data = null;
//        //boolean retried = false;

    System.out.printf("Loading Customer for DID=%d, WID=%d\n", d_id, w_id);
    int currentShard = 0;
    if (shardCount > 0) {
      currentShard = (w_id % shardCount);
      if (currentShard == 0) {
        currentShard = shardCount;
      }
    }

    final String[] CUSTOMER_COLUMNS = {
        "c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city",
        "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim",
        "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"
    };
    final Record customerRecord = new Record(21);
    final RecordLoader customerLoader = loadConfig.createLoader("customer", CUSTOMER_COLUMNS);

    final String[] HISTORY_COLUMN_NAME = {
        "h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"
    };
    final Record historyRecord = new Record(8);
    final RecordLoader historyLoader = loadConfig.createLoader("history", HISTORY_COLUMN_NAME);

    if ((currentShard == shardId) || (shardId == 0)) {
      //retry:
//            if (retried)
//                System.out.printf("Retrying ...\n");
//            retried = true;
      for (c_id = 1; c_id <= CUST_PER_DIST; c_id++) {

        /* Generate Customer Data */
        c_d_id = d_id;
        c_w_id = w_id;

        c_first = Util.makeAlphaString(8, 16);
        c_middle = "O" + "E";

        if (c_id <= 1000) {
          c_last = Util.lastName(c_id - 1);
        } else {
          c_last = Util.lastName(Util.nuRand(255, 0, 999));
        }

        c_street_1 = Util.makeAlphaString(10, 20);
        c_street_2 = Util.makeAlphaString(10, 20);
        c_city = Util.makeAlphaString(10, 20);
        c_state = Util.makeAlphaString(2, 2);
        c_zip = Util.makeAlphaString(9, 9);

        c_phone = Util.makeNumberString(16, 16);

        if (Util.randomNumber(0, 1) == 1)
          c_credit = "G";
        else
          c_credit = "B";
        c_credit += "C";

        c_credit_lim = 50000;
        c_discount = (float) (((float) Util.randomNumber(0, 50)) / 100.0);
        c_balance = (float) -10.0;

        c_data = Util.makeAlphaString(300, 500);
        //gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN); Java Equivalent below?
        Calendar calendar = Calendar.getInstance();
        //Date now = calendar.getTime();
        //Timestamp currentTimeStamp = new Timestamp(now.getTime());
        Date date = new java.sql.Date(calendar.getTimeInMillis());
                /*EXEC SQL INSERT INTO
                                        customer
                                        values(:c_id,:c_d_id,:c_w_id,
                                  :c_first,:c_middle,:c_last,
                                  :c_street_1,:c_street_2,:c_city,:c_state,
                                  :c_zip,
                                      :c_phone, :timestamp,
                                  :c_credit,
                                  :c_credit_lim,:c_discount,:c_balance,
                                  10.0, 1, 0,:c_data);*/
        try {


          customerRecord.reset();
          customerRecord.add(c_id);
          customerRecord.add(c_d_id);
          customerRecord.add(c_w_id);
          customerRecord.add(c_first);
          customerRecord.add(c_middle);
          customerRecord.add(c_last);
          customerRecord.add(c_street_1);
          customerRecord.add(c_street_2);
          customerRecord.add(c_city);
          customerRecord.add(c_state);
          customerRecord.add(c_zip);
          customerRecord.add(c_phone);
          customerRecord.add(date);
          customerRecord.add(c_credit);
          customerRecord.add(c_credit_lim);
          customerRecord.add(c_discount);
          customerRecord.add(c_balance);
          customerRecord.add(10.0);
          customerRecord.add(1);
          customerRecord.add(0);
          customerRecord.add(c_data);

          customerLoader.load(customerRecord);

        } catch (Exception e) {
          throw new RuntimeException("Customer insert error", e);
        }

        h_amount = 10.0;

        h_data = Util.makeAlphaString(12, 24);

                /*EXEC SQL INSERT INTO
                                        history
                                        values(:c_id,:c_d_id,:c_w_id,
                                       :c_d_id,:c_w_id, :timestamp,
                                       :h_amount,:h_data);*/
        try {

          historyRecord.reset();
          historyRecord.add(c_id);
          historyRecord.add(c_d_id);
          historyRecord.add(c_w_id);
          historyRecord.add(c_d_id);
          historyRecord.add(c_w_id);
          historyRecord.add(date);
          historyRecord.add(h_amount);
          historyRecord.add(h_data);

          historyLoader.load(historyRecord);

        } catch (Exception e) {
          throw new RuntimeException("Insert into History error", e);
        }
        if (optionDebug) {
          System.out.printf("CID = %d, LST = %s, P# = %s\n",
              c_id, c_last, c_phone);
        }
        if ((c_id % 100) == 0) {
          System.out.printf(".");
          if ((c_id % 1000) == 0)
            System.out.printf(" %d\n", c_id);
        }
      }

    }

    customerLoader.close();
    historyLoader.close();

    System.out.printf("Customer Done.\n");
  }

  /*
   * ==================================================================+ |
   * ROUTINE NAME |      Orders | DESCRIPTION |      Loads the Orders table |
   * Also loads the Order_Line table on the fly | ARGUMENTS |      w_id -
   * warehouse id
   * +==================================================================
   */
  public static void loadOrders(TpccLoadConfig loadConfig, int d_id, int w_id, int shardCount, int shardId) throws Exception {
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

    //TODO: shouldn't these variables be used?
//        float i_price;
//        float c_discount;

    float tmp_float;

    int currentShard = 0;
    if (shardCount > 0) {
      currentShard = (w_id % shardCount);
      if (currentShard == 0) {
        currentShard = shardCount;
      }
    }

    final String ORDERS_COLUMN_NAME[] = {"o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"};
    final Record orderRecord = new Record(8);
    final RecordLoader orderLoader = loadConfig.createLoader("orders", ORDERS_COLUMN_NAME);

    final String NEW_ORDERS_COLUMN_NAMES[] = {"no_o_id", "no_d_id", "no_w_id"};
    final Record newOrderRecord = new Record(3);
    final RecordLoader newOrderLoader = loadConfig.createLoader("new_orders", NEW_ORDERS_COLUMN_NAMES);

    final String ORDER_LINE_COLUMN_NAME[] = {"ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"};
    final Record orderLineRecord = new Record(10);
    final RecordLoader orderLineLoader = loadConfig.createLoader("order_line", ORDER_LINE_COLUMN_NAME);

    if ((currentShard == shardId) || (shardId == 0)) {
      System.out.printf("Loading Orders for D=%d, W=%d\n", d_id, w_id);
      o_d_id = d_id;
      o_w_id = w_id;
      Util.initPermutation();    /* initialize permutation of customer numbers */
      for (o_id = 1; o_id <= ORD_PER_DIST; o_id++) {

        /* Generate Order Data */
        o_c_id = Util.getPermutation();
        o_carrier_id = Util.randomNumber(1, 10);
        o_ol_cnt = Util.randomNumber(5, 15);

        //gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN); Java Equivalent below?
        Date date = new java.sql.Date(System.currentTimeMillis());


        if (o_id > 2100) {    /* the last 900 orders have not been
         * delivered) */
                    /*EXEC SQL INSERT INTO
                                         orders
                                         values(:o_id,:o_d_id,:o_w_id,:o_c_id,
                                        :timestamp,
                                        NULL,:o_ol_cnt, 1);*/

          orderRecord.reset();
          orderRecord.add(o_id);
          orderRecord.add(o_d_id);
          orderRecord.add(o_w_id);
          orderRecord.add(o_c_id);
          orderRecord.add(date);
          orderRecord.add(null);
          orderRecord.add(o_ol_cnt);
          orderRecord.add(1);

          orderLoader.load(orderRecord);


                    /*EXEC SQL INSERT INTO
                                         new_orders
                                         values(:o_id,:o_d_id,:o_w_id);*/
          newOrderRecord.reset();
          newOrderRecord.add(o_id);
          newOrderRecord.add(o_d_id);
          newOrderRecord.add(o_w_id);

          newOrderLoader.load(newOrderRecord);

        } else {
                    /*EXEC SQL INSERT INTO
                             orders
                             values(:o_id,:o_d_id,:o_w_id,:o_c_id,
                                :timestamp,
                                :o_carrier_id,:o_ol_cnt, 1);*/
          orderRecord.reset();
          orderRecord.add(o_id);
          orderRecord.add(o_d_id);
          orderRecord.add(o_w_id);
          orderRecord.add(o_c_id);
          orderRecord.add(date);
          orderRecord.add(o_carrier_id);
          orderRecord.add(o_ol_cnt);
          orderRecord.add(1);

          orderLoader.load(orderRecord);

        }


        if (optionDebug)
          System.out.printf("OID = %d, CID = %d, DID = %d, WID = %d\n",
              o_id, o_c_id, o_d_id, o_w_id);

        for (ol = 1; ol <= o_ol_cnt; ol++) {
          /* Generate Order Line Data */
          ol_i_id = Util.randomNumber(1, MAXITEMS);
          ol_supply_w_id = o_w_id;
          ol_quantity = 5;
          ol_amount = (float) 0.0;

          ol_dist_info = Util.makeAlphaString(24, 24);

          tmp_float = (float) ((float) (Util.randomNumber(10, 10000)) / 100.0);

          if (o_id > 2100) {
                        /*EXEC SQL INSERT INTO
                                              order_line
                                              values(:o_id,:o_d_id,:o_w_id,:ol,
                                             :ol_i_id,:ol_supply_w_id, NULL,
                                             :ol_quantity,:ol_amount,:ol_dist_info);*/
            orderLineRecord.reset();
            orderLineRecord.add(o_id);
            orderLineRecord.add(o_d_id);
            orderLineRecord.add(o_w_id);
            orderLineRecord.add(ol);
            orderLineRecord.add(ol_i_id);
            orderLineRecord.add(ol_supply_w_id);
            orderLineRecord.add(null);
            orderLineRecord.add(ol_quantity);
            orderLineRecord.add(ol_amount);
            orderLineRecord.add(ol_dist_info);

            orderLineLoader.load(orderLineRecord);

          } else {
                        /*EXEC SQL INSERT INTO
                                  order_line
                                  values(:o_id,:o_d_id,:o_w_id,:ol,
                                     :ol_i_id,:ol_supply_w_id,
                                     :timestamp,
                                     :ol_quantity,:tmp_float,:ol_dist_info);*/
            orderLineRecord.reset();
            orderLineRecord.add(o_id);
            orderLineRecord.add(o_d_id);
            orderLineRecord.add(o_w_id);
            orderLineRecord.add(ol);
            orderLineRecord.add(ol_i_id);
            orderLineRecord.add(ol_supply_w_id);
            orderLineRecord.add(date);
            orderLineRecord.add(ol_quantity);
            orderLineRecord.add(tmp_float);
            orderLineRecord.add(ol_dist_info);

            orderLineLoader.load(orderLineRecord);

          }

          if (optionDebug) {
            System.out.printf("OL = %d, IID = %d, QUAN = %d, AMT = %f\n",
                ol, ol_i_id, ol_quantity, ol_amount);
          }

        }
        if ((o_id % 100) == 0) {
          System.out.printf(".");

          if ((o_id % 1000) == 0) {
            System.out.printf(" %d\n", o_id);
          }
        }
      }

      orderLoader.close();
      orderLineLoader.close();
      newOrderLoader.close();
    }


    System.out.printf("Orders Done.\n");
  }
}
