package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Order {
  private final ObjectNode data;

  public Order(int o_id, int o_d_id, int o_w_id, Date orderDate, int maxItems, TPCCUtil util) {
    int o_c_id = util.getPermutation();
    int o_carrier_id = util.randomNumber(1, 10);
    int o_ol_cnt = util.randomNumber(5, 15);

    String date = util.dateToString(orderDate);

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    ArrayNode o_orderline = mapper.createArrayNode();
    this.data.put("o_id", o_id);
    this.data.put("o_d_id", o_d_id);
    this.data.put("o_w_id", o_w_id);
    this.data.put("o_c_id", o_c_id);
    this.data.put("o_entry_d", date);
    if (o_id > 2100) {
      this.data.putNull("o_carrier_id");
    } else {
      this.data.put("o_carrier_id", o_carrier_id);
    }
    this.data.put("o_ol_cnt", o_ol_cnt);
    this.data.put("o_all_local", 1);

    for (int ol = 1; ol <= o_ol_cnt; ol++) {
      ObjectNode ol_entry = mapper.createObjectNode();

      int ol_i_id = util.randomNumber(1, maxItems);
      int ol_quantity = 5;
      float ol_amount = (float) 0.0;

      String ol_dist_info = util.makeAlphaString(24, 24);

      float tmp_float = (float) ((float) (util.randomNumber(10, 10000)) / 100.0);

      Date startDate = util.addDays(2, orderDate);
      Date endDate = util.addDays(151, orderDate);
      String olData = util.randomDateText(startDate, endDate);

      ol_entry.put("ol_o_id", o_id);
      ol_entry.put("ol_d_id", o_d_id);
      ol_entry.put("ol_w_id", o_w_id);
      ol_entry.put("ol_number", ol);
      ol_entry.put("ol_i_id", ol_i_id);
      ol_entry.put("ol_supply_w_id", o_w_id);
      if (o_id > 2100) {
        ol_entry.putNull("ol_delivery_d");
        ol_entry.put("ol_amount", ol_amount);
      } else {
        ol_entry.put("ol_delivery_d", olData);
        ol_entry.put("ol_amount", tmp_float);
      }
      ol_entry.put("ol_quantity", ol_quantity);
      ol_entry.put("ol_dist_info", ol_dist_info);

      o_orderline.add(ol_entry);
    }

    this.data.set("o_orderline", o_orderline);
  }

  public int o_id() {
    return data.get("o_id").asInt();
  }

  public int o_d_id() {
    return data.get("o_d_id").asInt();
  }

  public int o_w_id() {
    return data.get("o_w_id").asInt();
  }

  public int o_c_id() {
    return data.get("o_c_id").asInt();
  }

  public int o_entry_d() {
    return data.get("o_entry_d").asInt();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
