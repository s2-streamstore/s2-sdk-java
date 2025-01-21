package s2.types;

import java.util.Optional;

public class ReadLimit {

  public final Optional<Long> count;
  public final Optional<Long> bytes;

  private ReadLimit(Optional<Long> count, Optional<Long> bytes) {
    this.count = count;
    this.bytes = bytes;
  }

  public ReadLimit remaining(long consumedRecords, long consumedBytes) {
    var newCount = count.map(count -> Math.max(count - consumedRecords, 0));
    var newBytes = bytes.map(bytes -> Math.max(bytes - consumedBytes, 0));
    return new ReadLimit(newCount, newBytes);
  }

  public void validateUnary() {
    this.count.ifPresent(
        count -> {
          if (count > 1000) {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid limit.count (%s) for unary request. Max count is 1000.", count));
          }
        });
    this.bytes.ifPresent(
        bytes -> {
          if (bytes > 1024 * 1024) {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid limit.bytes (%s) for unary request. Max bytes is 1MiB.", bytes));
          }
        });
  }

  public static final ReadLimit NONE = new ReadLimit(Optional.empty(), Optional.empty());

  // Static factory methods for different ways to instantiate ReadLimit
  public static ReadLimit count(long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Bytes must be positive");
    }
    return new ReadLimit(Optional.of(count), Optional.empty());
  }

  public static ReadLimit bytes(long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("Bytes must be positive");
    }
    return new ReadLimit(Optional.empty(), Optional.of(bytes));
  }

  public static ReadLimit countOrBytes(long count, long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("Bytes must be positive");
    }
    if (count < 0) {
      throw new IllegalArgumentException("Bytes must be positive");
    }
    return new ReadLimit(Optional.of(count), Optional.of(bytes));
  }

  public s2.v1alpha.ReadLimit toProto() {
    var builder = s2.v1alpha.ReadLimit.newBuilder();
    count.ifPresent(builder::setCount);
    bytes.ifPresent(builder::setBytes);
    return builder.build();
  }
}
