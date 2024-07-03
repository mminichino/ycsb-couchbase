package site.ycsb.tpc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class TPCCUtil {
  private final Random rand = new Random();
  private final String runTime = "2021-01-01 00:00:00";
  private Date runDate = new Date();
  private Date startDate = new Date();
  private Date endDate = new Date();
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
  private final NationData[] nations = {
      new NationData(48, "ALGERIA", 0), new NationData(49, "ARGENTINA", 1), new NationData(50, "BRAZIL", 1),
      new NationData(51, "CANADA", 1), new NationData(52, "EGYPT", 4), new NationData(53, "ETHIOPIA", 0),
      new NationData(54, "FRANCE", 3), new NationData(55, "GERMANY", 3), new NationData(56, "INDIA", 2),
      new NationData(57, "INDONESIA", 2), new NationData(65, "IRAN", 4), new NationData(66, "IRAQ", 4),
      new NationData(67, "JAPAN", 2), new NationData(68, "JORDAN", 4), new NationData(69, "KENYA", 0),
      new NationData(70, "MOROCCO", 0), new NationData(71, "MOZAMBIQUE", 0), new NationData(72, "PERU", 1),
      new NationData(73, "CHINA", 2), new NationData(74, "ROMANIA", 3), new NationData(75, "SAUDI ARABIA", 4),
      new NationData(76, "VIETNAM", 2), new NationData(77, "RUSSIA", 3), new NationData(78, "UNITED KINGDOM", 3),
      new NationData(79, "UNITED STATES", 1), new NationData(80, "CHINA", 2), new NationData(81, "PAKISTAN", 2),
      new NationData(82, "BANGLADESH", 2), new NationData(83, "MEXICO", 1), new NationData(84, "PHILIPPINES", 2),
      new NationData(85, "THAILAND", 2), new NationData(86, "ITALY", 3), new NationData(87, "SOUTH AFRICA", 0),
      new NationData(88, "SOUTH KOREA", 2), new NationData(89, "COLOMBIA", 1), new NationData(90, "SPAIN", 3),
      new NationData(97, "UKRAINE", 3), new NationData(98, "POLAND", 3), new NationData(99, "SUDAN", 0),
      new NationData(100, "UZBEKISTAN", 2), new NationData(101, "MALAYSIA", 2), new NationData(102, "VENEZUELA", 1),
      new NationData(103, "NEPAL", 2), new NationData(104, "AFGHANISTAN", 2), new NationData(105, "NORTH KOREA", 2),
      new NationData(106, "TAIWAN", 2), new NationData(107, "GHANA", 0), new NationData(108, "IVORY COAST", 0),
      new NationData(109, "SYRIA", 4), new NationData(110, "MADAGASCAR", 0), new NationData(111, "CAMEROON", 0),
      new NationData(112, "SRI LANKA", 2), new NationData(113, "ROMANIA", 3), new NationData(114, "NETHERLANDS", 3),
      new NationData(115, "CAMBODIA", 2), new NationData(116, "BELGIUM", 3), new NationData(117, "GREECE", 3),
      new NationData(118, "PORTUGAL", 3), new NationData(119, "ISRAEL", 4), new NationData(120, "FINLAND", 3),
      new NationData(121, "SINGAPORE", 2), new NationData(122, "NORWAY", 3)
  };
  private final String[] regions = {"Africa", "America", "Asia", "Europe", "Middle East"};
  private final String[] tpchNouns = {
      "foxes", "ideas", "theodolites", "pinto beans", "instructions", "dependencies", "excuses",
      "platelets", "asymptotes", "courts", "dolphins", "multipliers", "sauternes", "warthogs",
      "frets", "dinos", "attainments", "somas", "Tiresias'", "patterns", "forges", "braids",
      "hockey players", "frays", "warhorses", "dugouts", "notornis", "epitaphs", "pearls",
      "tithes", "waters", "orbits", "gifts", "sheaves", "depths", "sentiments", "decoys",
      "realms", "pains", "grouches", "escapades"
  };

  private final String[] tpchVerbs = {
      "sleep", "wake", "are", "cajole", "haggle", "nag", "use", "boost", "affix", "detect",
      "integrate", "maintain", "nod", "was", "lose", "sublate", "solve", "thrash", "promise",
      "engage", "hinder", "print", "x-ray", "breach", "eat", "grow", "impress", "mold",
      "poach", "serve", "run", "dazzle", "snooze", "doze", "unwind", "kindle", "play", "hang",
      "believe", "doubt"
  };

  private final String[] tpchAdjectives = {
      "furious", "sly", "careful", "blithe", "quick", "fluffy", "slow", "quiet", "ruthless",
      "thin", "close", "dogged", "daring", "brave", "stealthy", "permanent", "enticing", "idle",
      "busy", "regular", "final", "ironic", "even", "bold", "silent"
  };

  private final String[] tpchAdverbs = {
      "sometimes", "always", "never", "furiously", "slyly", "carefully", "blithely", "quickly",
      "fluffily", "slowly", "quietly", "ruthlessly", "thinly", "closely", "doggedly", "daringly",
      "bravely", "stealthily", "permanently", "enticingly", "idly", "busily", "regularly",
      "finally", "ironically", "evenly", "boldly", "silently"
  };

  private final String[] tpchPrepositions = {
      "about", "above", "according to", "across", "after", "against", "along", "alongside of",
      "among", "around", "at", "atop", "before", "behind", "beneath", "beside", "besides",
      "between", "beyond", "by", "despite", "during", "except", "for", "from", "in place of",
      "inside", "instead of", "into", "near", "of", "on", "outside", "over", "past", "since",
      "through", "throughout", "to", "toward", "under", "until", "up", "upon", "without",
      "with", "within"
  };

  private static final String[] tpchAuxiliaries = {
      "do", "may", "might", "shall", "will", "would", "can", "could", "should", "ought to",
      "must", "will have to", "shall have to", "could have to", "should have to", "must have to",
      "need to", "try to"
  };

  private static final String[] tpchTerminators = {".", ";", ":", "?", "!", "--"};

  public TPCCUtil(int cust, int ord) {
    custPerDist = cust;
    ordPerDist = ord;
    nums = new int[custPerDist];

    String dateFormat = "%Y-%m-%d %H:%M:%S";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    try {
      runDate = timeStampFormat.parse(runTime);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    endDate = subtractDays(1, runDate);
    startDate = subtractYears(7, endDate);
  }

  private Date subtractDays(int days, Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.DATE, -days);
    return c.getTime();
  }

  private Date subtractYears(int years, Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.YEAR, -years);
    return c.getTime();
  }

  public Date addSeconds(int seconds, Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.SECOND, seconds);
    return c.getTime();
  }

  public Date addDays(int days, Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.DATE, days);
    return c.getTime();
  }

  public Date randomDate(Date start, Date end) {
    long deltaSecs = (start.getTime() - end.getTime()) / 1000;
    int randOffset = randomNumber(1, Math.toIntExact(deltaSecs));
    return addSeconds(randOffset, start);
  }

  public Date randomDate() {
    long deltaSecs = (startDate.getTime() - endDate.getTime()) / 1000;
    int randOffset = randomNumber(1, Math.toIntExact(deltaSecs));
    return addSeconds(randOffset, startDate);
  }

  public Date getStartDate() {
    return startDate;
  }

  public Date getEndDate() {
    return endDate;
  }

  public String randomDateText(Date start, Date end) {
    String dateFormat = "%Y-%m-%d %H:%M:%S";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    return timeStampFormat.format(randomDate(start, end));
  }

  public String startDateText() {
    String dateFormat = "%Y-%m-%d %H:%M:%S";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    return timeStampFormat.format(startDate);
  }

  public String endDateText() {
    String dateFormat = "%Y-%m-%d %H:%M:%S";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    return timeStampFormat.format(endDate);
  }

  public String dateToString(Date date) {
    String dateFormat = "%Y-%m-%d %H:%M:%S";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    return timeStampFormat.format(date);
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

  public String makeRandomString(int minValue, int maxValue) {
    String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    String lower = "abcdefghijklmnopqrstuvwxyz";
    String characters = upper + lower;
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
  
  public NationData getNation(int id) {
    return nations[id-1];
  }
  
  public int numNations() {
    return nations.length;
  }

  public String getRegion(int id) {
    return regions[id-1];
  }

  public int numRegions() {
    return regions.length;
  }

  public String strLeadingZero(int i, int zeros) {
    return String.format("%0" + zeros + "d", i);
  }

  public double roundDouble(double value, int places) {
    BigDecimal bd;
    bd = BigDecimal.valueOf(value);
    bd = bd.setScale(places, RoundingMode.HALF_UP);
    return bd.doubleValue();
  }

  public double randomDouble(double minValue, double maxValue, int places) {
    double randomValue = minValue + (maxValue - minValue) * rand.nextDouble();
    return roundDouble(randomValue, places);
  }

  public String addDouble(double minValue, double maxValue, int places) {
    return String.valueOf(randomDouble(minValue, maxValue, places));
  }

  public int randomArrayIndex(int length) {
    int minValue = 0;
    int maxValue = length - 1;
    return rand.nextInt((maxValue - minValue) + 1) + minValue;
  }

  public String tpchNounPhrase() {
    switch (randomNumber(0, 3)) {
      case 0:
        return tpchNoun();
      case 1:
        return tpchAdjective() + " " + tpchNoun();
      case 2:
        return tpchAdjective() + ", " + tpchAdjective() + " " + tpchNoun();
      default:
        return tpchAdverb() + " " + tpchAdjective() + " " + tpchNoun();
    }
  }

  public String tpchVerbPhrase() {
    switch (randomNumber(0, 3)) {
      case 0:
        return tpchVerb();
      case 1:
        return tpchAuxiliary() + " " + tpchVerb();
      case 2:
        return tpchVerb() + " " + tpchAdverb();
      default:
        return tpchAuxiliary() + " " + tpchVerb() + " " + tpchAdverb();
    }
  }

  public String tpchPrepositionalPhrase() {
    return tpchPrepositions[randomArrayIndex(tpchPrepositions.length)] + " the " + tpchNounPhrase();
  }

  public String tpchNoun() {
    return tpchNouns[randomArrayIndex(tpchNouns.length)];
  }

  public String tpchVerb() {
    return tpchVerbs[randomArrayIndex(tpchVerbs.length)];
  }

  public String tpchAdverb() {
    return tpchAdverbs[randomArrayIndex(tpchAdverbs.length)];
  }

  public String tpchAdjective() {
    return tpchAdjectives[randomArrayIndex(tpchAdjectives.length)];
  }

  public String tpchAuxiliary() {
    return tpchAuxiliaries[randomArrayIndex(tpchAuxiliaries.length)];
  }

  public String tpchTerminator() {
    return tpchTerminators[randomArrayIndex(tpchTerminators.length)];
  }

  public String tpchSentence() {
    switch (randomNumber(0, 4)) {
      case 0:
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " + tpchTerminator();
      case 1:
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " + tpchPrepositionalPhrase() + " " + tpchTerminator();
      case 2:
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " + tpchNounPhrase() + " " + tpchTerminator();
      case 3:
        return tpchNounPhrase() + " " + tpchPrepositionalPhrase() + " " + tpchVerbPhrase() + " "
            + tpchNounPhrase() + " " + tpchTerminator();
      default:
        return tpchNounPhrase() + " " + tpchPrepositionalPhrase() + " " + tpchVerbPhrase() + " " +
            tpchPrepositionalPhrase() + " " + tpchTerminator();
    }
  }

  public String tpchText(int length) {
    StringBuilder s = new StringBuilder();
    int i = 0;
    while (s.length() < length) {
      s.append(i++ == 0 ? "" : " ").append(tpchSentence());
    }
    return s.toString();
  }

  public String tpchTextString(int minLength, int maxLength) {
    int length = randomNumber(minLength, maxLength);
    return tpchText(length);
  }

  public String tpchTextStringCustomer(int minLength, int maxLength, String action) {
    StringBuilder s = new StringBuilder();
    int offset = 8 + action.length();
    int window = maxLength - minLength - offset;
    int l1 = randomNumber(0, window);
    int l2 = randomNumber(0, window - l1);
    int l3 = window - l2 - l1;
    s.append(tpchText(l1));
    s.append("Customer");
    s.append(tpchText(l2));
    s.append(action);
    s.append(tpchText(l3));
    return s.toString();
  }

  public List<List<Integer>> getRandomSets(int values, int length) {
    List<Integer> list = IntStream.range(1, values + 1).boxed().collect(Collectors.toList());
    Collections.shuffle(list);
    int fullChunks = (list.size() - 1) / length;
    List<List<Integer>> chunks = IntStream.range(0, fullChunks + 1).mapToObj(
        n -> list.subList(n * length, n == fullChunks ? list.size() : (n + 1) * length)).collect(Collectors.toList());
    List<List<Integer>> result = new ArrayList<>();
    result.add(chunks.get(0));
    result.add(chunks.get(1));
    return result;
  }

  private TPCCUtil() {}
}
