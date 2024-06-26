package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

public class OrderLine {
  private final ObjectNode data;

  public OrderLine(int ol, int o_id, int o_d_id, int o_w_id, int maxItems, TPCCUtil util) {
    int ol_i_id = util.randomNumber(1, maxItems);
    int ol_quantity = 5;
    float ol_amount = (float) 0.0;

    String ol_dist_info = util.makeAlphaString(24, 24);

    float tmp_float = (float) ((float) (util.randomNumber(10, 10000)) / 100.0);

    String dateFormat = "yy-MM-dd'T'HH:mm:ss";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    String date = timeStampFormat.format(new Date());

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("ol_o_id", o_id);
    this.data.put("ol_d_id", o_d_id);
    this.data.put("ol_w_id", o_w_id);
    this.data.put("ol_number", ol);
    this.data.put("ol_i_id", ol_i_id);
    this.data.put("ol_supply_w_id", o_w_id);
    if (o_id > 2100) {
      this.data.putNull("ol_delivery_d");
      this.data.put("ol_amount", ol_amount);
    } else {
      this.data.put("ol_delivery_d", date);
      this.data.put("ol_amount", tmp_float);
    }
    this.data.put("ol_quantity", ol_quantity);
    this.data.put("ol_dist_info", ol_dist_info);
  }

  public int ol_o_id() {
    return this.data.get("ol_o_id").asInt();
  }

  public int ol_d_id() {
    return this.data.get("ol_d_id").asInt();
  }

  public int ol_w_id() {
    return this.data.get("ol_w_id").asInt();
  }

  public int ol_number() {
    return this.data.get("ol_number").asInt();
  }

  public int ol_i_id() {
    return this.data.get("ol_i_id").asInt();
  }

  public int ol_supply_w_id() {
    return this.data.get("ol_supply_w_id").asInt();
  }

  public String ol_delivery_d() {
    return this.data.get("ol_delivery_d").asText();
  }

  public double ol_amount() {
    return this.data.get("ol_amount").asDouble();
  }

  public int ol_quantity() {
    return this.data.get("ol_quantity").asInt();
  }

  public String ol_dist_info() {
    return this.data.get("ol_dist_info").asText();
  }

  public String asJson() {
    return data.toString();
  }
}
