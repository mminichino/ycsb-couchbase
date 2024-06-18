package site.ycsb.tpc;

import java.util.Random;

public final class TPCCUtil {
  private final Random rand = new Random();
  private final String[] lastNameParts = {
      "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE",
      "ANTI", "CALLY", "ATION", "EING"
  };
  private final int C_255 = randomNumber(0, 255);
  private final int C_1023 = randomNumber(0, 1023);
  private final int C_8191 = randomNumber(0, 8191);
  private int custPerDist;
  private int ordPerDist;
  private int permCount;
  private int[] nums;

  public TPCCUtil(int cust, int ord) {
    custPerDist = cust;
    ordPerDist = ord;
    nums = new int[custPerDist];
  }

  public int randomNumber(int minValue, int maxValue) {
    return rand.nextInt((maxValue - minValue) + 1) + minValue;
  }

  public String makeAlphaString(int minValue, int maxValue) {
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

  public String lastName(int num) {
    String name;

    name = lastNameParts[num / 100];
    name = name + lastNameParts[(num / 10) % 10];
    name = name + lastNameParts[num % 10];

    return name;
  }

  public int nuRand(int A, int x, int y) {
    int C;
    switch (A) {
      case 255:
        C = C_255;
        break;
      case 1023:
        C = C_1023;
        break;
      case 8191:
        C = C_8191;
        break;
      default:
        throw new RuntimeException("randomNonUniformInt: unexpected value for A (%d)\n" + A);
    }
    return ((((randomNumber(0, A) | randomNumber(x, y)) + C) % (y - x + 1)) + x);
  }

  public String makeNumberString(int minValue, int maxValue) {
    String numbers = "0123456789";
    char[] alphanum = numbers.toCharArray();
    int length = randomNumber(minValue, maxValue);
    int max = alphanum.length - 1;

    StringBuilder string = new StringBuilder();
    for (int i = 0; i < length; i++) {
      string.append(alphanum[randomNumber(0, max)]);
    }

    return string.toString();
  }

  public void initPermutation() {
    int i, j;
    int[] tempNums = new int[custPerDist];
    permCount = 0;

    for (i = 0; i < ordPerDist; i++) {
      nums[i] = i + 1;
      tempNums[i] = i + 1;
    }

    for (i = 0; i < ordPerDist - 1; i++) {
      j = randomNumber(i + 1, ordPerDist - 1);
      nums[j] = tempNums[i];
    }
  }

  public int getPermutation() {
    if (permCount >= ordPerDist) {
      throw new RuntimeException("GetPermutation: past end of list");
    }
    return nums[permCount++];
  }

  private TPCCUtil() {}
}
