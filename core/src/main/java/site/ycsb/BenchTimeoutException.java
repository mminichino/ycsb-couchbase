package site.ycsb;

public class BenchTimeoutException extends Exception
{
  public BenchTimeoutException() {}

  public BenchTimeoutException(String message)
  {
    super(message);
  }
}
