package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

public class History {
  private final ObjectNode data;

  public History(int c_id, int c_d_id, int c_w_id, int custPerDist, int ordPerDist) {
    TPCCUtil util = new TPCCUtil(custPerDist, ordPerDist);

    String dateFormat = "yy-MM-dd'T'HH:mm:ss";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    String date = timeStampFormat.format(new Date());

    double h_amount = 10.0;
    String h_data = util.makeAlphaString(12, 24);

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("h_c_id", c_id);
    this.data.put("h_c_d_id", c_d_id);
    this.data.put("h_c_w_id", c_w_id);
    this.data.put("h_d_id", c_d_id);
    this.data.put("h_w_id", c_w_id);
    this.data.put("h_date", date);
    this.data.put("h_amount", h_amount);
    this.data.put("h_data", h_data);
  }

  public int h_c_id() {
    return data.get("h_c_id").asInt();
  }

  public int h_c_d_id() {
    return data.get("h_c_d_id").asInt();
  }

  public int h_c_w_id() {
    return data.get("h_c_w_id").asInt();
  }

  public int h_d_id() {
    return data.get("h_d_id").asInt();
  }

  public int h_w_id() {
    return data.get("h_w_id").asInt();
  }

  public String h_date() {
    return data.get("h_date").asText();
  }

  public double h_amount() {
    return data.get("h_amount").asDouble();
  }

  public String h_data() {
    return data.get("h_data").asText();
  }

  public String asJson() {
    return data.toString();
  }
}
