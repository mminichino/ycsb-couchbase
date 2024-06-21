package site.ycsb.tpc;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;
import site.ycsb.SQLDB;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TPCCNewOrder {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.TPCCNewOrder");

  private String s_dist_01 = null;
  private String s_dist_02 = null;
  private String s_dist_03 = null;
  private String s_dist_04 = null;
  private String s_dist_05 = null;
  private String s_dist_06 = null;
  private String s_dist_07 = null;
  private String s_dist_08 = null;
  private String s_dist_09 = null;
  private String s_dist_10 = null;

  String[] iname;
  String[] bg;
  float[] amt;
  float[] price;
  int[] stock;
  int[] ol_num_seq;
  boolean joins;

  boolean debug = false;

  TPCCStatements pStmts;

  public void NewOrder(int maxNumItems, boolean joins) {
    iname = new String[maxNumItems];
    bg = new String[maxNumItems];
    amt = new float[maxNumItems];
    price = new float[maxNumItems];
    stock = new int[maxNumItems];
    ol_num_seq = new int[maxNumItems];
    this.joins = joins;
  }

  private String pickDistInfo(String ol_dist_info, int ol_supply_w_id) {
    switch (ol_supply_w_id) {
      case 1:
        ol_dist_info = s_dist_01;
        break;
      case 2:
        ol_dist_info = s_dist_02;
        break;
      case 3:
        ol_dist_info = s_dist_03;
        break;
      case 4:
        ol_dist_info = s_dist_04;
        break;
      case 5:
        ol_dist_info = s_dist_05;
        break;
      case 6:
        ol_dist_info = s_dist_06;
        break;
      case 7:
        ol_dist_info = s_dist_07;
        break;
      case 8:
        ol_dist_info = s_dist_08;
        break;
      case 9:
        ol_dist_info = s_dist_09;
        break;
      case 10:
        ol_dist_info = s_dist_10;
        break;
    }

    return ol_dist_info;
  }

  public int newOrder(SQLDB db,
                      int w_id_arg,
                      int d_id_arg,
                      int c_id_arg,
                      int o_ol_cnt_arg,
                      int o_all_local_arg,
                      int[] itemid,
                      int[] supware,
                      int[] qty
  ) {
    try {
      int w_id = w_id_arg;
      int d_id = d_id_arg;
      int c_id = c_id_arg;
      int o_ol_cnt = o_ol_cnt_arg;
      int o_all_local = o_all_local_arg;
      float c_discount = 0;
      String c_last = null;
      String c_credit = null;
      float w_tax = 0;
      int d_next_o_id = 0;
      float d_tax = 0;
      int o_id = 0;
      String i_name = null;
      float i_price = 0;
      String i_data = null;
      int ol_i_id = 0;
      int s_quantity = 0;
      String s_data = null;

      String ol_dist_info = null;
      int ol_supply_w_id = 0;
      float ol_amount = 0;
      int ol_number = 0;
      int ol_quantity = 0;

      int min_num = 0;
      int i = 0, j = 0, tmp = 0, swp = 0;

      String dateFormat = "yy-MM-dd'T'HH:mm:ss";
      SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
      String currentTimeStamp = timeStampFormat.format(new Date());

      if (joins) {
        try {
          String statement = pStmts.getStatement(0);
          ArrayList<Object> parameters = new ArrayList<>();
          parameters.add(w_id);
          parameters.add(w_id);
          parameters.add(d_id);
          parameters.add(c_id);

          if (debug)
            LOGGER.debug("SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = {} AND c_w_id = {} AND c_d_id = {} AND c_id = {}", w_id, w_id, d_id, c_id);

          List<Map<String, ?>> rs = db.select(statement, parameters);
          c_discount = (float) rs.get(0).get("c_discount");
          c_last = (String) rs.get(0).get("c_last");
          c_credit = (String) rs.get(0).get("c_credit");
          w_tax = (float) rs.get(0).get("w_tax");
        } catch (Throwable e) {
          LOGGER.error("SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = {} AND c_w_id = {} AND c_d_id = {} AND c_id = {}", w_id, w_id, d_id, c_id, e);
          return 0;
        }
      } else {
        try {
          String statement1 = pStmts.getStatement(35);
          ArrayList<Object> parameters1 = new ArrayList<>();
          parameters1.add(w_id);
          parameters1.add(d_id);
          parameters1.add(c_id);

          String statement2 = pStmts.getStatement(36);
          ArrayList<Object> parameters2 = new ArrayList<>();
          parameters2.add(w_id);

          if (debug)
            LOGGER.debug("SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", w_id, d_id, c_id);
          if (debug)
            LOGGER.debug("SELECT w_tax FROM warehouse WHERE w_id = {}", w_id);

          List<Map<String, ?>> rs1 = db.select(statement1, parameters1);
          c_discount = (float) rs1.get(0).get("c_discount");
          c_last = (String) rs1.get(0).get("c_last");
          c_credit = (String) rs1.get(0).get("c_credit");

          List<Map<String, ?>> rs2 = db.select(statement1, parameters1);
          w_tax = (float) rs2.get(0).get("w_tax");
        } catch (Throwable e) {
          LOGGER.error("SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", w_id, d_id, c_id, e);
          return 0;
        }
      }

      try {
        String statement = pStmts.getStatement(1);
        ArrayList<Object> parameters = new ArrayList<>();
        parameters.add(d_id);
        parameters.add(w_id);

        if (debug)
          LOGGER.debug("SELECT d_next_o_id, d_tax FROM district WHERE d_id = {}  AND d_w_id = {}", d_id, w_id);

        List<Map<String, ?>> rs = db.select(statement, parameters);
        d_next_o_id = (int) rs.get(0).get("d_next_o_id");
        d_tax = (float) rs.get(0).get("d_tax");
      } catch (Exception e) {
        LOGGER.error("SELECT d_next_o_id, d_tax FROM district WHERE d_id = {}  AND d_w_id = {} FOR UPDATE", d_id, w_id, e);
        return 0;
      }

      try {
        final PreparedStatement pstmt2 = pStmts.getStatement(2);
        pstmt2.setInt(1, d_next_o_id);
        pstmt2.setInt(2, d_id);
        pstmt2.setInt(3, w_id);
        if (TRACE)
          logger.trace("UPDATE district SET d_next_o_id = " + d_next_o_id + " + 1 WHERE d_id = " + d_id + " AND d_w_id = " + w_id);
        pstmt2.executeUpdate();


      } catch (SQLException e) {
        logger.error("UPDATE district SET d_next_o_id = " + d_next_o_id + " + 1 WHERE d_id = " + d_id + " AND d_w_id = " + w_id, e);
        throw new Exception("NewOrder update transaction error", e);
      }

      o_id = d_next_o_id;

      try {
        final PreparedStatement pstmt3 = pStmts.getStatement(3);
        pstmt3.setInt(1, o_id);
        pstmt3.setInt(2, d_id);
        pstmt3.setInt(3, w_id);
        pstmt3.setInt(4, c_id);
        pstmt3.setString(5, currentTimeStamp);
        pstmt3.setInt(6, o_ol_cnt);
        pstmt3.setInt(7, o_all_local);
        if (TRACE)
          logger.trace("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) " +
              "VALUES(" + o_id + "," + d_id + "," + w_id + "," + c_id + "," + currentTimeStamp + "," + o_ol_cnt + "," + o_all_local + ")");
        pstmt3.executeUpdate();


      } catch (SQLException e) {
        logger.error("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) " +
            "VALUES(" + o_id + "," + d_id + "," + w_id + "," + c_id + "," + currentTimeStamp + "," + o_ol_cnt + "," + o_all_local + ")", e);
        throw new Exception("NewOrder insert transaction error", e);
      }

      //Get prepared statement
      //"INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)
      try {
        final PreparedStatement pstmt4 = pStmts.getStatement(4);
        pstmt4.setInt(1, o_id);
        pstmt4.setInt(2, d_id);
        pstmt4.setInt(3, w_id);
        if (TRACE)
          logger.trace("INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (" + o_id + "," + d_id + "," + w_id + ")");
        pstmt4.executeUpdate();


      } catch (SQLException e) {
        logger.error("INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (" + o_id + "," + d_id + "," + w_id + ")", e);
        throw new Exception("NewOrder insert transaction error", e);
      }

      /* sort orders to avoid DeadLock */
      for (i = 0; i < o_ol_cnt; i++) {
        ol_num_seq[i] = i;
      }

      for (i = 0; i < (o_ol_cnt - 1); i++) {
        tmp = (MAXITEMS + 1) * supware[ol_num_seq[i]] + itemid[ol_num_seq[i]];
        min_num = i;
        for (j = i + 1; j < o_ol_cnt; j++) {
          if ((MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]] < tmp) {
            tmp = (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]];
            min_num = j;
          }
        }
        if (min_num != i) {
          swp = ol_num_seq[min_num];
          ol_num_seq[min_num] = ol_num_seq[i];
          ol_num_seq[i] = swp;
        }
      }

      for (ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
        ol_supply_w_id = supware[ol_num_seq[ol_number - 1]];
        ol_i_id = itemid[ol_num_seq[ol_number - 1]];
        ol_quantity = qty[ol_num_seq[ol_number - 1]];

        /* EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; */
        //Get prepared statement
        //"SELECT i_price, i_name, i_data FROM item WHERE i_id = ?"

        try {
          final PreparedStatement pstmt5 = pStmts.getStatement(5);
          pstmt5.setInt(1, ol_i_id);
          if (TRACE) logger.trace("SELECT i_price, i_name, i_data FROM item WHERE i_id =" + ol_i_id);
          try (ResultSet rs = pstmt5.executeQuery()) {
            if (rs.next()) {
              i_price = rs.getFloat(1);
              i_name = rs.getString(2);
              i_data = rs.getString(3);
            } else {
              if (DEBUG) {
                logger.debug("No item found for item id " + ol_i_id);
              }
              throw new AbortedTransactionException();
            }
          }
        } catch (SQLException e) {
          logger.error("SELECT i_price, i_name, i_data FROM item WHERE i_id =" + ol_i_id, e);
          throw new Exception("NewOrder select transaction error", e);
        }

        price[ol_num_seq[ol_number - 1]] = i_price;
        iname[ol_num_seq[ol_number - 1]] = i_name;

        //Get prepared statement
        //"SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE"
        try {
          final PreparedStatement pstmt6 = pStmts.getStatement(6);
          pstmt6.setInt(1, ol_i_id);
          pstmt6.setInt(2, ol_supply_w_id);
          if (TRACE)
            logger.trace("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM " +
                "stock WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id + " FOR UPDATE");

          try (ResultSet rs = pstmt6.executeQuery()) {
            if (rs.next()) {
              s_quantity = rs.getInt(1);
              s_data = rs.getString(2);
              s_dist_01 = rs.getString(3);
              s_dist_02 = rs.getString(4);
              s_dist_03 = rs.getString(5);
              s_dist_04 = rs.getString(6);
              s_dist_05 = rs.getString(7);
              s_dist_06 = rs.getString(8);
              s_dist_07 = rs.getString(9);
              s_dist_08 = rs.getString(10);
              s_dist_09 = rs.getString(11);
              s_dist_10 = rs.getString(12);
            }
          }

        } catch (SQLException e) {
          logger.error("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM " +
              "stock WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id + " FOR UPDATE", e);
          throw new Exception("NewOrder select transaction error", e);
        }

        ol_dist_info = pickDistInfo(ol_dist_info, d_id);    /* pick correct * s_dist_xx */

        stock[ol_num_seq[ol_number - 1]] = s_quantity;

        if ((i_data.contains("original")) && (s_data.contains("original"))) {
          bg[ol_num_seq[ol_number - 1]] = "B";

        } else {
          bg[ol_num_seq[ol_number - 1]] = "G";

        }

        if (s_quantity > ol_quantity) {
          s_quantity = s_quantity - ol_quantity;
        } else {
          s_quantity = s_quantity - ol_quantity + 91;
        }

        //Get the prepared statement
        //"UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?"
        try {
          final PreparedStatement pstmt7 = pStmts.getStatement(7);
          pstmt7.setInt(1, s_quantity);
          pstmt7.setInt(2, ol_i_id);
          pstmt7.setInt(3, ol_supply_w_id);
          if (TRACE)
            logger.trace("UPDATE stock SET s_quantity = " + s_quantity + " WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id);
          pstmt7.executeUpdate();


        } catch (SQLException e) {
          logger.error("UPDATE stock SET s_quantity = " + s_quantity + " WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id, e);
          throw new Exception("NewOrder update transaction error", e);
        }

        ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
        amt[ol_num_seq[ol_number - 1]] = ol_amount;


        //Get prepared statement
        //"INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

        try {
          final PreparedStatement pstmt8 = pStmts.getStatement(8);
          pstmt8.setInt(1, o_id);
          pstmt8.setInt(2, d_id);
          pstmt8.setInt(3, w_id);
          pstmt8.setInt(4, ol_number);
          pstmt8.setInt(5, ol_i_id);
          pstmt8.setInt(6, ol_supply_w_id);
          pstmt8.setInt(7, ol_quantity);
          pstmt8.setFloat(8, ol_amount);
          pstmt8.setString(9, ol_dist_info);
          if (TRACE)
            logger.trace("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) " +
                "VALUES (" + o_id + "," + d_id + "," + w_id + "," + ol_number + "," + ol_i_id + "," + ol_supply_w_id + "," + ol_quantity + ","
                + ol_amount + "," + ol_dist_info + ")");
          pstmt8.executeUpdate();


        } catch (SQLException e) {
          logger.error("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) " +
              "VALUES (" + o_id + "," + d_id + "," + w_id + "," + ol_number + "," + ol_i_id + "," + ol_supply_w_id + "," + ol_quantity + ","
              + ol_amount + "," + ol_dist_info + ")", e);
          throw new Exception("NewOrder insert transaction error", e);
        }

      }
      // Commit.
      pStmts.commit();

      return 1;
    } catch (AbortedTransactionException ate) {
      // Rollback if an aborted transaction, they are intentional in some percentage of cases.
      if (logger.isDebugEnabled()) {
        logger.debug("Caught AbortedTransactionException");
      }
      pStmts.rollback();
      return 1; // this is not an error!
    } catch (Exception e) {
      logger.error("New Order error", e);
      pStmts.rollback();
      return 0;
    }


  }
}
