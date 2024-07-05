package site.ycsb.tpc.tpcc;

import site.ycsb.BenchRun;
import site.ycsb.BenchWorkload;
import site.ycsb.Status;

public class RunDriver extends BenchWorkload {
  @Override
  public boolean test(BenchRun db, Object threadState) {
    for (String query : Queries.sqlStatements) {
      System.out.println(query);
      return true;
    }
    return false;
  }

  @Override
  public boolean run(BenchRun db, Object threadState) {
    return false;
  }
}
