package site.ycsb.workloads;

import site.ycsb.SQLDB;
import site.ycsb.SQLWorkload;

public class TPCC extends SQLWorkload {
  @Override
  public boolean load(SQLDB db, Object threadState) {
    return false;
  }

  @Override
  public boolean run(SQLDB db, Object threadState) {
    return false;
  }
}
