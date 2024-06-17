package site.ycsb.tpc;

import java.util.Random;

public class TPCCUtil {
  private static final Random rand = new Random();

  public static int randomNumber(int minValue, int maxValue) {
    return rand.nextInt((maxValue - minValue) + 1) + minValue;
  }

  public static String makeAlphaString(int minValue, int maxValue) {
    String numbers = "0123456789";
    String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    String lower = "abcdefghijklmnopqrstuvwxyz";
    String characters = upper + lower + numbers;
    char[] alphanum = characters.toCharArray();
    int length = randomNumber(minValue, maxValue);
    int max = alphanum.length - 1;

    StringBuilder string = new StringBuilder();
    for (int i = 0; i < length; i++) {
      string.append(alphanum[randomNumber(0, max)]);
    }

    return string.toString();
  }
}
