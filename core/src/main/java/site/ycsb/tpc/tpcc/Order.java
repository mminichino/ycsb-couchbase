package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Order {
  private final ObjectNode data;

  public Order(int o_id, int o_d_id, int o_w_id, Date orderDate, TPCCUtil util) {
    int o_c_id = util.getPermutation();
    int o_carrier_id = util.randomNumber(1, 10);
    int o_ol_cnt = util.randomNumber(5, 15);

    String dateFormat = "yy-MM-dd'T'HH:mm:ss";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    String date = util.dateToString(orderDate);

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
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
