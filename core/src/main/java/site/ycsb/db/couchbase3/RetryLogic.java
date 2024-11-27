package site.ycsb.db.couchbase3;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.Callable;
import org.slf4j.LoggerFactory;

/**
 * Retry Method.
 */
public final class RetryLogic {
  private static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("com.couchbase.RetryLogic");

  public static <T>T retryBlock(Callable<T> block) throws Exception {
    int retryCount = 10;
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      try {
        return block.call();
      } catch (Exception e) {
        LOGGER.error("Retry count {}: {}: error: {}", retryCount, e.getClass(), e.getMessage());
        Writer buffer = new StringWriter();
        PrintWriter pw = new PrintWriter(buffer);
        e.printStackTrace(pw);
        LOGGER.error("{}", buffer);
        if (retryNumber == retryCount) {
          throw e;
        } else {
          double factor = waitFactor * Math.pow(2, retryNumber);
          long wait = (long) factor;
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    return block.call();
  }

  public static void retryVoid(Runnable block) throws Exception {
    int retryCount = 10;
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      try {
        block.run();
      } catch (Exception e) {
        LOGGER.debug("Retry count: {} error: {}", retryCount, e.getMessage(), e);
        if (retryNumber == retryCount) {
          throw e;
        } else {
          double factor = waitFactor * retryNumber;
          long wait = (long) factor;
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  private RetryLogic() {
    super();
  }
}
