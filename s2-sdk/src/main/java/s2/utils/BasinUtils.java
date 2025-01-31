package s2.utils;

import java.util.regex.Pattern;

public class BasinUtils {

  private static final Pattern BASIN_NAME_REGEX = Pattern.compile("[0-9a-z][0-9a-z-]*");

  public static void validateBasinName(String basinName) {
    if (basinName.length() < 8) {
      throw new IllegalArgumentException("Basin name must be at least 8 characters");
    }
    if (basinName.length() > 48) {
      throw new IllegalArgumentException("Basin name must be at most 48 characters");
    }
    if (!BASIN_NAME_REGEX.matcher(basinName).matches()) {
      throw new IllegalArgumentException(
          "Basin name must comprise lowercase letters, numbers, and hyphens. It cannot begin or end with a hyphen");
    }
  }
}
