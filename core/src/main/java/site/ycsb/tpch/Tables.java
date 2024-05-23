package site.ycsb.tpch;

import site.ycsb.DataType;

import java.util.*;

public class Tables {
  Map<String, DataType> PartTable = new HashMap<>() {{
    put("p_partkey", DataType.IDENTIFIER);
    put("p_name", DataType.VARIABLE_STRING.setLength(55));
    put("p_mfgr", DataType.FIXED_STRING.setLength(25));
    put("p_brand", DataType.FIXED_STRING.setLength(10));
    put("p_type", DataType.VARIABLE_STRING.setLength(25));
    put("p_size", DataType.INTEGER);
    put("p_container", DataType.FIXED_STRING.setLength(10));
    put("p_retailprice", DataType.FLOAT);
    put("p_comment", DataType.VARIABLE_STRING.setLength(23));
  }};
  Set<String> PartKeys = new HashSet<>(List.of("p_partkey"));
  Integer PartScaleFactor = 200000;

  Map<String, DataType> SupplierTable = new HashMap<>() {{
    put("s_suppkey", DataType.IDENTIFIER);
    put("s_name", DataType.FIXED_STRING.setLength(25));
    put("s_address", DataType.VARIABLE_STRING.setLength(40));
    put("s_nationkey", DataType.IDENTIFIER);
    put("s_phone", DataType.FIXED_STRING.setLength(15));
    put("s_acctbal", DataType.FLOAT);
    put("s_comment", DataType.VARIABLE_STRING.setLength(101));
  }};
  Set<String> SupplierKeys = new HashSet<>(List.of("s_suppkey"));
  Integer SupplierScaleFactor = 10000;

  Map<String, DataType> PartSuppTable = new HashMap<>() {{
    put("ps_partkey", DataType.IDENTIFIER);
    put("ps_suppkey", DataType.IDENTIFIER);
    put("ps_availqty", DataType.INTEGER);
    put("ps_supplycost", DataType.FLOAT);
    put("ps_comment", DataType.VARIABLE_STRING.setLength(199));
  }};
  Set<String> PartSuppKeys = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));

  Map<String, DataType> CustomerTable = new HashMap<>() {{
    put("c_custkey", DataType.IDENTIFIER);
    put("c_name", DataType.VARIABLE_STRING.setLength(25));
    put("c_address", DataType.VARIABLE_STRING.setLength(40));
    put("c_nationkey", DataType.IDENTIFIER);
    put("c_phone", DataType.FIXED_STRING.setLength(15));
    put("c_acctbal", DataType.FLOAT);
    put("c_mktsegment", DataType.FIXED_STRING.setLength(10));
    put("c_comment", DataType.VARIABLE_STRING.setLength(117));
  }};
  Set<String> CustomerKeys = new HashSet<>(List.of("c_custkey"));
  Integer CustomerScaleFactor = 150000;

  Map<String, DataType> OrdersTable = new HashMap<>() {{
    put("o_orderkey", DataType.IDENTIFIER);
    put("o_custkey", DataType.IDENTIFIER);
    put("o_orderstatus", DataType.FIXED_STRING.setLength(1));
    put("o_totalprice", DataType.FLOAT);
    put("o_orderdate", DataType.DATE);
    put("o_orderpriority", DataType.FIXED_STRING.setLength(15));
    put("o_clerk", DataType.FIXED_STRING.setLength(15));
    put("o_shippriority", DataType.INTEGER);
    put("o_comment", DataType.VARIABLE_STRING.setLength(79));
  }};
  Set<String> OrdersKeys = new HashSet<>(List.of("o_orderkey"));
  Integer OrdersScaleFactor = 1500000;

  Map<String, DataType> LineItemTable = new HashMap<>() {{
    put("l_orderkey", DataType.IDENTIFIER);
    put("l_partkey", DataType.IDENTIFIER);
    put("l_suppkey", DataType.IDENTIFIER);
    put("l_linenumber", DataType.INTEGER);
    put("l_quantity", DataType.FLOAT);
    put("l_extendedprice", DataType.FLOAT);
    put("l_discount", DataType.FLOAT);
    put("l_tax", DataType.FLOAT);
    put("l_returnflag", DataType.FIXED_STRING.setLength(1));
    put("l_linestatus", DataType.FIXED_STRING.setLength(1));
    put("l_shipdate", DataType.DATE);
    put("l_commitdate", DataType.DATE);
    put("l_receiptdate", DataType.DATE);
    put("l_shipinstruct", DataType.FIXED_STRING.setLength(25));
    put("l_shipmode", DataType.FIXED_STRING.setLength(10));
    put("l_comment", DataType.VARIABLE_STRING.setLength(44));
  }};
  Set<String> LineItemKeys = new HashSet<>(List.of("l_orderkey", "l_linenumber"));

  Map<String, DataType> NationTable = new HashMap<>() {{
    put("n_nationkey", DataType.IDENTIFIER);
    put("n_name", DataType.FIXED_STRING.setLength(25));
    put("n_regionkey", DataType.IDENTIFIER);
    put("n_comment", DataType.VARIABLE_STRING.setLength(152));
  }};
  Set<String> NationKeys = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));

  Map<String, DataType> RegionTable = new HashMap<>() {{
    put("r_regionkey", DataType.IDENTIFIER);
    put("r_name", DataType.FIXED_STRING.setLength(25));
    put("r_comment", DataType.VARIABLE_STRING.setLength(152));
  }};
  Set<String> RegionKeys = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));
}
