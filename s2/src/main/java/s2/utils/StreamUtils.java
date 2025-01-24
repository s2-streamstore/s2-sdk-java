package s2.utils;

public class StreamUtils {

  public static void validateStreamName(String streamName) {

    if (streamName == null || streamName.isEmpty()) {
      throw new IllegalArgumentException("No stream name provided.");
    }

    if (streamName.length() > 512) {
      throw new IllegalArgumentException(
          "Stream name is too long. Must be 512 characters or less.");
    }
  }
}
